package presenter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/Nomadcxx/sysc-notify/internal/state"
	"github.com/Nomadcxx/sysc-notify/protocol"
)

const (
	socketName                 = "presenter.v1.sock"
	RequiredCapability         = "notification-state"
	RequiredLifetimeCapability = "presentation-lifetime"
	handshakeTimeout           = 5 * time.Second
)

type Server struct {
	runtimeDir string
	uid        uint32
	peerUID    atomic.Uint32

	mu         sync.Mutex
	owner      *state.Owner
	listener   *net.UnixListener
	socketPath string
	current    *connection
	preparing  *connection
	nextGen    uint64
	started    bool

	stop      chan struct{}
	done      chan error
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func NewAt(runtimeDir string) *Server {
	uid := uint32(os.Geteuid())
	s := &Server{runtimeDir: runtimeDir, uid: uid, stop: make(chan struct{}), done: make(chan error, 1)}
	s.peerUID.Store(uid)
	return s
}

func New() *Server { return NewAt(os.Getenv("XDG_RUNTIME_DIR")) }

func (s *Server) Serve(owner *state.Owner) error {
	if s == nil || owner == nil {
		return errors.New("presenter: missing server dependency")
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("presenter: server already started")
	}
	s.started = true
	s.owner = owner
	s.mu.Unlock()

	listener, path, err := listenRuntime(s.runtimeDir, s.uid)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = listener
	s.socketPath = path
	s.mu.Unlock()
	s.wg.Add(1)
	go s.acceptLoop()
	return nil
}

func (s *Server) SocketPath() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.socketPath
}

func (s *Server) Done() <-chan error { return s.done }

func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	var result error
	s.closeOnce.Do(func() {
		close(s.stop)
		s.mu.Lock()
		listener, current, preparing, path := s.listener, s.current, s.preparing, s.socketPath
		s.mu.Unlock()
		if listener != nil {
			result = errors.Join(result, listener.Close())
		}
		if current != nil {
			current.fail()
		}
		if preparing != nil && preparing != current {
			preparing.fail()
		}
		s.wg.Wait()
		if path != "" {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				result = errors.Join(result, err)
			}
		}
		close(s.done)
	})
	return result
}

func (s *Server) Publish(event state.Event) bool {
	if event.Delta == nil {
		return true
	}
	if event.Sequence == 0 || event.Delta.Validate() != nil {
		return false
	}
	payload, err := marshalEnvelope(string(event.Delta.Kind), 0, event.Sequence, *event.Delta)
	if err != nil {
		return false
	}
	message := outbound{data: payload, sequence: event.Sequence}
	s.mu.Lock()
	preparing, current := s.preparing, s.current
	var prepared, published = true, true
	if preparing != nil {
		prepared = preparing.prepare(message)
	}
	if current != nil {
		published = current.enqueueDelta(message)
	}
	s.mu.Unlock()
	return prepared && published
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.AcceptUnix()
		if err != nil {
			select {
			case <-s.stop:
				return
			default:
				select {
				case s.done <- fmt.Errorf("presenter: accept: %w", err):
				default:
				}
				return
			}
		}
		s.wg.Add(1)
		go s.handle(conn)
	}
}

func (s *Server) handle(socket *net.UnixConn) {
	defer s.wg.Done()
	c := newConnection(socket, 0)
	writerStarted := false
	defer func() {
		c.fail()
		if writerStarted {
			<-c.writerDone
		}
		s.connectionDone(c)
	}()
	uid, err := peerUID(socket)
	if err != nil || uid != s.peerUID.Load() {
		return
	}
	if err := socket.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return
	}
	hello, err := readHello(socket)
	if err != nil || !hasCapability(hello.Capabilities, RequiredCapability) ||
		!hasCapability(hello.Capabilities, RequiredLifetimeCapability) {
		return
	}
	if err := socket.SetDeadline(time.Time{}); err != nil {
		return
	}

	s.mu.Lock()
	if s.preparing != nil {
		s.mu.Unlock()
		return
	}
	s.nextGen++
	if s.nextGen == 0 {
		s.nextGen++
	}
	c.generation = s.nextGen
	s.preparing = c
	owner := s.owner
	s.mu.Unlock()

	snapshot, err := owner.Snapshot(context.Background())
	if err != nil || snapshot.Validate() != nil {
		return
	}
	serviceHello := protocol.Hello{
		Major: protocol.ProtocolMajor, Minor: protocol.ProtocolMinor, Role: protocol.RolePresenter,
		Capabilities: []string{RequiredCapability, RequiredLifetimeCapability, "actions", "history", "inline-reply"},
	}
	helloFrame, err := marshalEnvelope(protocol.KindHello, 0, 0, serviceHello)
	if err != nil {
		return
	}
	snapshotFrame, err := marshalEnvelope(protocol.KindSnapshot, 0, snapshot.Sequence, snapshot)
	if err != nil {
		return
	}
	if err := socket.SetWriteDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return
	}
	if err := protocol.WriteFrame(socket, helloFrame); err != nil {
		return
	}
	if err := protocol.WriteFrame(socket, snapshotFrame); err != nil {
		return
	}
	if err := socket.SetWriteDeadline(time.Time{}); err != nil {
		return
	}

	s.mu.Lock()
	if s.preparing != c || !c.activate(snapshot.Sequence) {
		if s.preparing == c {
			s.preparing = nil
		}
		s.mu.Unlock()
		return
	}
	old := s.current
	s.current = c
	s.preparing = nil
	s.mu.Unlock()
	if old != nil {
		old.fail()
		_, _ = owner.Do(context.Background(), state.Command{Kind: state.PresenterLost, Generation: old.generation})
	}

	writerStarted = true
	go c.writeLoop()
	c.readLoop(owner)
}

func (s *Server) connectionDone(c *connection) {
	s.mu.Lock()
	wasCurrent := s.current == c
	if wasCurrent {
		s.current = nil
	}
	if s.preparing == c {
		s.preparing = nil
	}
	owner := s.owner
	s.mu.Unlock()
	if wasCurrent && owner != nil {
		_, _ = owner.Do(context.Background(), state.Command{Kind: state.PresenterLost, Generation: c.generation})
	}
}

func listenRuntime(runtimeDir string, uid uint32) (*net.UnixListener, string, error) {
	if runtimeDir == "" {
		return nil, "", errors.New("presenter: XDG_RUNTIME_DIR is empty")
	}
	abs, err := filepath.Abs(runtimeDir)
	if err != nil {
		return nil, "", err
	}
	if err := rejectSymlinkComponents(abs); err != nil {
		return nil, "", err
	}
	if err := validateDirectory(abs, uid, 0o700); err != nil {
		return nil, "", err
	}
	dir := filepath.Join(abs, "sysc-notify")
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, "", fmt.Errorf("presenter: create runtime directory: %w", err)
	}
	if err := validateDirectory(dir, uid, 0o700); err != nil {
		return nil, "", err
	}
	path := filepath.Join(dir, socketName)
	if err := removeStaleSocket(path, uid); err != nil {
		return nil, "", err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, "", fmt.Errorf("presenter: listen: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(path)
		return nil, "", fmt.Errorf("presenter: secure socket: %w", err)
	}
	if err := validateSocket(path, uid); err != nil {
		_ = listener.Close()
		_ = os.Remove(path)
		return nil, "", err
	}
	return listener, path, nil
}

func validateDirectory(path string, uid uint32, mode os.FileMode) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("presenter: inspect directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm() != mode || fileUID(info) != uid {
		return fmt.Errorf("presenter: unsafe runtime directory %q (mode %o uid %d, want mode %o uid %d)", path, info.Mode().Perm(), fileUID(info), mode, uid)
	}
	return nil
}

func validateSocket(path string, uid uint32) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || fileUID(info) != uid {
		return fmt.Errorf("presenter: unsafe socket %q", path)
	}
	return nil
}

func removeStaleSocket(path string, uid uint32) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || fileUID(info) != uid {
		return fmt.Errorf("presenter: unsafe existing socket %q", path)
	}
	conn, dialErr := net.DialTimeout("unix", path, 50*time.Millisecond)
	if dialErr == nil {
		_ = conn.Close()
		return fmt.Errorf("presenter: socket already active")
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("presenter: remove stale socket: %w", err)
	}
	return nil
}

func rejectSymlinkComponents(path string) error {
	current := string(filepath.Separator)
	relative := strings.TrimPrefix(filepath.Clean(path), current)
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("presenter: symlink path component %q", current)
		}
	}
	return nil
}

func fileUID(info os.FileInfo) uint32 {
	return info.Sys().(*syscall.Stat_t).Uid
}

func peerUID(conn *net.UnixConn) (uint32, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var credential *unix.Ucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credential, controlErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return 0, err
	}
	if controlErr != nil {
		return 0, controlErr
	}
	return credential.Uid, nil
}

func readHello(conn net.Conn) (protocol.Hello, error) {
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		return protocol.Hello{}, err
	}
	var envelope protocol.Envelope
	if err := protocol.DecodeStrict(frame, &envelope); err != nil {
		return protocol.Hello{}, err
	}
	if err := envelope.Validate(); err != nil || envelope.Kind != protocol.KindHello {
		return protocol.Hello{}, errors.New("presenter: expected hello")
	}
	var hello protocol.Hello
	if err := protocol.DecodeStrict(envelope.Payload, &hello); err != nil {
		return protocol.Hello{}, err
	}
	if err := hello.Validate(protocol.RolePresenter); err != nil {
		return protocol.Hello{}, err
	}
	return hello, nil
}

func hasCapability(capabilities []string, required string) bool {
	for _, capability := range capabilities {
		if capability == required {
			return true
		}
	}
	return false
}
