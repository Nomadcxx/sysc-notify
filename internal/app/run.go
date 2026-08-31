package app

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/godbus/dbus/v5"

	"github.com/Nomadcxx/sysc-notify/internal/fdo"
	"github.com/Nomadcxx/sysc-notify/internal/history"
	"github.com/Nomadcxx/sysc-notify/internal/presenter"
	"github.com/Nomadcxx/sysc-notify/internal/state"
)

type Config struct {
	RuntimeDir string
	StateHome  string
	ProcRoot   string
	Ready      chan struct{}
}

func Run(ctx context.Context, config Config) error {
	if ctx == nil {
		return errors.New("app: missing context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if config.RuntimeDir == "" {
		config.RuntimeDir = os.Getenv("XDG_RUNTIME_DIR")
	}
	if config.ProcRoot == "" {
		config.ProcRoot = "/proc"
	}

	var store *history.Store
	var err error
	if config.StateHome == "" {
		store, err = history.Open(time.Now())
	} else {
		store, err = history.OpenAt(config.StateHome, time.Now())
	}
	if err != nil {
		return err
	}

	presentation := presenter.NewAt(config.RuntimeDir)
	sink := &eventSink{sinks: []state.Sink{presentation}}
	owner := state.StartWithHistory(nil, sink, store)
	if err := presentation.Serve(owner); err != nil {
		return errors.Join(err, shutdown(owner, presentation, nil, nil))
	}

	conn, err := dbus.ConnectSessionBus()
	if err != nil {
		return errors.Join(err, shutdown(owner, presentation, nil, nil))
	}
	service := fdo.NewAt(conn, config.ProcRoot)
	sink.add(service)
	if err := service.Serve(owner); err != nil {
		return errors.Join(err, shutdown(owner, presentation, service, conn))
	}
	if config.Ready != nil {
		close(config.Ready)
	}

	var runErr error
	select {
	case <-ctx.Done():
	case err, ok := <-presentation.Done():
		if ok {
			runErr = err
		} else {
			runErr = errors.New("app: presenter stopped")
		}
	case err, ok := <-service.Done():
		if ok {
			runErr = err
		} else {
			runErr = errors.New("app: D-Bus service stopped")
		}
	}
	return errors.Join(runErr, shutdown(owner, presentation, service, conn))
}

func shutdown(owner *state.Owner, presentation *presenter.Server, service *fdo.Server, conn *dbus.Conn) error {
	var err error
	if presentation != nil {
		err = errors.Join(err, presentation.Close())
	}
	if service != nil {
		err = errors.Join(err, service.Close())
	}
	if owner != nil {
		err = errors.Join(err, owner.Close())
	}
	if conn != nil {
		err = errors.Join(err, conn.Close())
	}
	return err
}

type eventSink struct {
	mu    sync.RWMutex
	sinks []state.Sink
}

func (s *eventSink) add(sink state.Sink) {
	s.mu.Lock()
	s.sinks = append(s.sinks, sink)
	s.mu.Unlock()
}

func (s *eventSink) Publish(event state.Event) bool {
	s.mu.RLock()
	sinks := append([]state.Sink(nil), s.sinks...)
	s.mu.RUnlock()
	ok := true
	for _, sink := range sinks {
		if !sink.Publish(event) {
			ok = false
		}
	}
	return ok
}
