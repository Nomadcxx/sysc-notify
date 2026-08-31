package fdo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/godbus/dbus/v5"

	"github.com/Nomadcxx/sysc-notify/internal/notify"
	"github.com/Nomadcxx/sysc-notify/internal/state"
	"github.com/Nomadcxx/sysc-notify/protocol"
)

const (
	BusName       = "org.freedesktop.Notifications"
	Interface     = BusName
	ServerName    = "sysc-notify"
	ServerVendor  = "Nomadcxx"
	ServerVersion = "0.1.0"
	SpecVersion   = "1.3"

	dbusInvalidArgs     = "org.freedesktop.DBus.Error.InvalidArgs"
	dbusFailed          = "org.freedesktop.DBus.Error.Failed"
	invalidNotification = "org.freedesktop.Notifications.InvalidNotification"
)

const ObjectPath dbus.ObjectPath = "/org/freedesktop/Notifications"

type Server struct {
	conn *dbus.Conn

	mu        sync.RWMutex
	owner     *state.Owner
	attempted bool
	served    bool

	signals     chan *dbus.Signal
	stop        chan struct{}
	done        chan error
	monitorDone chan struct{}
	closeOnce   sync.Once
	closing     atomic.Bool
}

type endpoint struct {
	server *Server
}

func New(conn *dbus.Conn) *Server {
	return &Server{
		conn: conn, signals: make(chan *dbus.Signal, 8), stop: make(chan struct{}),
		done: make(chan error, 1), monitorDone: make(chan struct{}),
	}
}

func (s *Server) Serve(owner *state.Owner) error {
	if s == nil || s.conn == nil || owner == nil {
		return errors.New("fdo: missing server dependency")
	}
	s.mu.Lock()
	if s.attempted {
		s.mu.Unlock()
		return errors.New("fdo: server already started")
	}
	s.attempted = true
	s.owner = owner
	s.mu.Unlock()

	if err := s.conn.Export(endpoint{server: s}, ObjectPath, Interface); err != nil {
		return fmt.Errorf("fdo: export interface: %w", err)
	}
	if err := s.conn.AddMatchSignal(nameOwnerMatch()...); err != nil {
		_ = s.conn.Export(nil, ObjectPath, Interface)
		return fmt.Errorf("fdo: watch name ownership: %w", err)
	}
	s.conn.Signal(s.signals)
	reply, err := s.conn.RequestName(BusName, dbus.NameFlagDoNotQueue)
	if err != nil || reply != dbus.RequestNameReplyPrimaryOwner {
		s.conn.RemoveSignal(s.signals)
		_ = s.conn.RemoveMatchSignal(nameOwnerMatch()...)
		_ = s.conn.Export(nil, ObjectPath, Interface)
		if err != nil {
			return fmt.Errorf("fdo: acquire %s: %w", BusName, err)
		}
		return fmt.Errorf("fdo: acquire %s: %s", BusName, reply)
	}
	s.mu.Lock()
	s.served = true
	s.mu.Unlock()
	go s.monitorName(s.conn.Names()[0])
	return nil
}

func (s *Server) Done() <-chan error { return s.done }

func (s *Server) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	var result error
	s.closeOnce.Do(func() {
		s.closing.Store(true)
		close(s.stop)
		s.mu.RLock()
		served := s.served
		s.mu.RUnlock()
		if served {
			<-s.monitorDone
			s.conn.RemoveSignal(s.signals)
			result = errors.Join(result, s.conn.RemoveMatchSignal(nameOwnerMatch()...))
			result = errors.Join(result, s.conn.Export(nil, ObjectPath, Interface))
			reply, err := s.conn.ReleaseName(BusName)
			if err != nil {
				result = errors.Join(result, err)
			} else if reply != dbus.ReleaseNameReplyReleased && reply != dbus.ReleaseNameReplyNotOwner {
				result = errors.Join(result, fmt.Errorf("fdo: release %s: %s", BusName, reply))
			}
		}
	})
	return result
}

func (s *Server) Publish(event state.Event) bool {
	if s == nil || s.conn == nil {
		return false
	}
	var err error
	if event.Delta != nil && event.Delta.Kind == protocol.DeltaClosed {
		err = errors.Join(err, s.conn.Emit(ObjectPath, Interface+".NotificationClosed", event.Delta.ID, uint32(event.Delta.CloseReason)))
	}
	if event.Action != nil {
		err = errors.Join(err, s.conn.Emit(ObjectPath, Interface+".ActionInvoked", event.Action.ID, event.Action.Key))
	}
	if event.Reply != nil {
		err = errors.Join(err, s.conn.Emit(ObjectPath, Interface+".NotificationReplied", event.Reply.ID, event.Reply.Text))
	}
	return err == nil
}

func (s *Server) monitorName(uniqueName string) {
	defer close(s.monitorDone)
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			return
		case <-s.conn.Context().Done():
			if !s.closing.Load() {
				s.done <- fmt.Errorf("fdo: D-Bus connection lost: %w", s.conn.Context().Err())
			}
			return
		case signal := <-s.signals:
			if signal == nil || signal.Name != "org.freedesktop.DBus.NameOwnerChanged" || len(signal.Body) != 3 {
				continue
			}
			name, nameOK := signal.Body[0].(string)
			oldOwner, oldOK := signal.Body[1].(string)
			newOwner, newOK := signal.Body[2].(string)
			if nameOK && oldOK && newOK && name == BusName && oldOwner == uniqueName && newOwner != uniqueName && !s.closing.Load() {
				s.done <- errors.New("fdo: notification bus name lost")
				return
			}
		}
	}
}

func nameOwnerMatch() []dbus.MatchOption {
	return []dbus.MatchOption{
		dbus.WithMatchSender("org.freedesktop.DBus"),
		dbus.WithMatchInterface("org.freedesktop.DBus"),
		dbus.WithMatchMember("NameOwnerChanged"),
		dbus.WithMatchArg(0, BusName),
	}
}

func (e endpoint) GetCapabilities() ([]string, *dbus.Error) {
	return []string{}, nil
}

func (e endpoint) GetServerInformation() (string, string, string, string, *dbus.Error) {
	return ServerName, ServerVendor, ServerVersion, SpecVersion, nil
}

func (e endpoint) Notify(sender dbus.Sender, appName string, replacesID uint32, appIcon, summary, body string,
	actions []string, variants map[string]dbus.Variant, expireTimeout int32,
) (uint32, *dbus.Error) {
	hints, err := convertHints(variants)
	if err != nil {
		return 0, busError(dbusInvalidArgs, err)
	}
	var pid uint32
	if err := e.server.conn.BusObject().Call("org.freedesktop.DBus.GetConnectionUnixProcessID", 0, string(sender)).Store(&pid); err != nil {
		return 0, busError(dbusFailed, fmt.Errorf("query sender PID: %w", err))
	}
	candidate, err := notify.Normalize(notify.Request{
		AppName: appName, ReplacesID: replacesID, AppIcon: appIcon, Summary: summary, Body: body,
		Actions: actions, Hints: hints, ExpireTimeout: expireTimeout,
		Sender: notify.Sender{Name: string(sender), PID: pid},
	})
	if err != nil {
		return 0, busError(dbusInvalidArgs, err)
	}
	owner := e.server.stateOwner()
	if owner == nil {
		return 0, busError(dbusFailed, errors.New("notification state unavailable"))
	}
	result, err := owner.Do(context.Background(), state.Command{Kind: state.Add, Candidate: candidate})
	if err != nil {
		return 0, busError(dbusFailed, err)
	}
	return result.ID, nil
}

func (e endpoint) CloseNotification(id uint32) *dbus.Error {
	owner := e.server.stateOwner()
	if owner == nil {
		return busError(dbusFailed, errors.New("notification state unavailable"))
	}
	_, err := owner.Do(context.Background(), state.Command{Kind: state.CloseRequested, ID: id})
	if errors.Is(err, state.ErrNotFound) {
		return busError(invalidNotification, err)
	}
	if err != nil {
		return busError(dbusFailed, err)
	}
	return nil
}

func (s *Server) stateOwner() *state.Owner {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.owner
}

func busError(name string, err error) *dbus.Error {
	return &dbus.Error{Name: name, Body: []any{err.Error()}}
}
