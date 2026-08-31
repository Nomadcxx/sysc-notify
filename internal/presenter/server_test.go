package presenter

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Nomadcxx/sysc-notify/internal/notify"
	"github.com/Nomadcxx/sysc-notify/internal/state"
	"github.com/Nomadcxx/sysc-notify/protocol"
)

func TestServerCreatesPrivateRuntimeSocketAndRejectsUnsafePaths(t *testing.T) {
	h := startServerHarness(t, nil)
	for path, mode := range map[string]os.FileMode{
		filepath.Join(h.runtimeDir, "sysc-notify"):             0o700,
		filepath.Join(h.runtimeDir, "sysc-notify", socketName): 0o600,
	} {
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != mode {
			t.Fatalf("mode %s = %o, want %o", path, got, mode)
		}
	}

	realRuntime := t.TempDir()
	linkedRuntime := filepath.Join(filepath.Dir(realRuntime), "linked-runtime")
	if err := os.Symlink(realRuntime, linkedRuntime); err != nil {
		t.Fatal(err)
	}
	unsafe := NewAt(linkedRuntime)
	unsafeOwner := state.Start(nil, unsafe)
	t.Cleanup(func() { _ = unsafeOwner.Close() })
	if err := unsafe.Serve(unsafeOwner); err == nil {
		t.Fatal("symlink runtime directory succeeded")
	}

	wrongOwner := NewAt(privateTempDir(t))
	wrongOwner.uid = uint32(os.Geteuid()) + 1
	wrongOwnerState := state.Start(nil, wrongOwner)
	t.Cleanup(func() { _ = wrongOwnerState.Close() })
	if err := wrongOwner.Serve(wrongOwnerState); err == nil {
		t.Fatal("wrong-owner runtime directory succeeded")
	}
}

func TestServerRejectsPeerUIDBeforeHello(t *testing.T) {
	h := startServerHarness(t, nil)
	h.server.peerUID.Store(uint32(os.Geteuid()) + 1)
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: h.server.SocketPath(), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	_ = writeEnvelope(conn, protocol.KindHello, 0, 0, validHello())
	if _, err := protocol.ReadFrame(conn); err == nil {
		t.Fatal("wrong-UID peer reached hello response")
	}
}

func TestServerRejectsIncompatibleHello(t *testing.T) {
	for name, hello := range map[string]protocol.Hello{
		"major":      {Major: protocol.ProtocolMajor + 1, Minor: protocol.ProtocolMinor, Role: protocol.RolePresenter, Capabilities: []string{RequiredCapability}},
		"capability": {Major: protocol.ProtocolMajor, Minor: protocol.ProtocolMinor, Role: protocol.RolePresenter},
	} {
		t.Run(name, func(t *testing.T) {
			h := startServerHarness(t, nil)
			conn := dialSocket(t, h.server.SocketPath())
			defer conn.Close()
			if err := writeEnvelope(conn, protocol.KindHello, 0, 0, hello); err != nil {
				t.Fatal(err)
			}
			if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := protocol.ReadFrame(conn); err == nil {
				t.Fatal("invalid hello received a response")
			}
		})
	}
}

func TestPresenterReceivesSnapshotThenNextDelta(t *testing.T) {
	h := startServerHarness(t, nil)
	first := addCandidate(t, h.owner, "before", 0)
	client := connectPresenter(t, h.server.SocketPath())
	defer client.conn.Close()
	if client.snapshot.Sequence != 1 || len(client.snapshot.Active) != 1 || client.snapshot.Active[0].ID != first {
		t.Fatalf("initial snapshot = %#v", client.snapshot)
	}
	second := addCandidate(t, h.owner, "after", 0)
	envelope, delta := readDelta(t, client.conn)
	if envelope.Sequence != client.snapshot.Sequence+1 || delta.Kind != protocol.DeltaAdded || delta.Notification == nil || delta.Notification.ID != second {
		t.Fatalf("next delta = %#v, %#v", envelope, delta)
	}
}

func TestSecondPresenterReplacesGeneration(t *testing.T) {
	h := startServerHarness(t, nil)
	addCandidate(t, h.owner, "record", 0)
	first := connectPresenter(t, h.server.SocketPath())
	defer first.conn.Close()
	second := connectPresenter(t, h.server.SocketPath())
	defer second.conn.Close()
	if err := first.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.ReadFrame(first.conn); err == nil {
		t.Fatal("first presenter remained connected")
	}
	if len(second.snapshot.Active) != 1 {
		t.Fatalf("replacement snapshot = %#v", second.snapshot)
	}
}

type serverHarness struct {
	server     *Server
	owner      *state.Owner
	runtimeDir string
}

func startServerHarness(t *testing.T, clock state.Clock) serverHarness {
	t.Helper()
	runtimeDir := privateTempDir(t)
	server := NewAt(runtimeDir)
	owner := state.Start(clock, server)
	if err := server.Serve(owner); err != nil {
		_ = owner.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Close()
		_ = owner.Close()
	})
	return serverHarness{server: server, owner: owner, runtimeDir: runtimeDir}
}

func privateTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "pn-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

type presenterClient struct {
	conn     *net.UnixConn
	snapshot protocol.Snapshot
}

func connectPresenter(t *testing.T, path string) presenterClient {
	t.Helper()
	conn := dialSocket(t, path)
	if err := writeEnvelope(conn, protocol.KindHello, 0, 0, validHello()); err != nil {
		t.Fatal(err)
	}
	serverHelloEnvelope := readEnvelope(t, conn)
	if serverHelloEnvelope.Kind != protocol.KindHello {
		t.Fatalf("first server message = %#v", serverHelloEnvelope)
	}
	var hello protocol.Hello
	decodePayload(t, serverHelloEnvelope, &hello)
	if err := hello.Validate(protocol.RolePresenter); err != nil {
		t.Fatal(err)
	}
	snapshotEnvelope := readEnvelope(t, conn)
	if snapshotEnvelope.Kind != protocol.KindSnapshot {
		t.Fatalf("second server message = %#v", snapshotEnvelope)
	}
	var snapshot protocol.Snapshot
	decodePayload(t, snapshotEnvelope, &snapshot)
	if snapshot.Sequence != snapshotEnvelope.Sequence {
		t.Fatalf("snapshot sequence %d != envelope %d", snapshot.Sequence, snapshotEnvelope.Sequence)
	}
	return presenterClient{conn: conn, snapshot: snapshot}
}

func validHello() protocol.Hello {
	return protocol.Hello{
		Major: protocol.ProtocolMajor, Minor: protocol.ProtocolMinor, Role: protocol.RolePresenter,
		Capabilities: []string{RequiredCapability},
	}
}

func dialSocket(t *testing.T, path string) *net.UnixConn {
	t.Helper()
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func writeEnvelope(conn net.Conn, kind string, requestID, sequence uint64, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	envelope, err := json.Marshal(protocol.Envelope{Kind: kind, RequestID: requestID, Sequence: sequence, Payload: raw})
	if err != nil {
		return err
	}
	return protocol.WriteFrame(conn, envelope)
}

func readEnvelope(t *testing.T, conn net.Conn) protocol.Envelope {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	var envelope protocol.Envelope
	if err := protocol.DecodeStrict(frame, &envelope); err != nil {
		t.Fatal(err)
	}
	if err := envelope.Validate(); err != nil {
		t.Fatal(err)
	}
	return envelope
}

func readDelta(t *testing.T, conn net.Conn) (protocol.Envelope, protocol.Delta) {
	t.Helper()
	envelope := readEnvelope(t, conn)
	var delta protocol.Delta
	decodePayload(t, envelope, &delta)
	return envelope, delta
}

func decodePayload(t *testing.T, envelope protocol.Envelope, destination any) {
	t.Helper()
	if err := protocol.DecodeStrict(envelope.Payload, destination); err != nil {
		t.Fatal(err)
	}
}

func addCandidate(t *testing.T, owner *state.Owner, summary string, timeout time.Duration) uint32 {
	t.Helper()
	result, err := owner.Do(context.Background(), state.Command{Kind: state.Add, Candidate: notify.Candidate{
		Summary: summary, Urgency: protocol.UrgencyNormal, ExpireTimeout: int32(timeout / time.Millisecond),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return result.ID
}
