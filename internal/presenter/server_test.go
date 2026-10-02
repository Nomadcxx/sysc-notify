package presenter

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
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

func TestTransientAcceptErrorDoesNotStopTheServer(t *testing.T) {
	runtimeDir := privateTempDir(t)
	server := NewAt(runtimeDir)
	var calls atomic.Int32
	server.accept = func() (*net.UnixConn, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("temporary emfile")
		}
		return server.listener.AcceptUnix()
	}
	owner := state.Start(nil, server)
	if err := server.Serve(owner); err != nil {
		_ = owner.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Close()
		_ = owner.Close()
	})

	select {
	case err := <-server.Done():
		t.Fatalf("transient accept error stopped presenter: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	client := connectPresenter(t, server.SocketPath())
	defer client.conn.Close()
	if client.hello.Role != protocol.RolePresenter {
		t.Fatalf("hello after accept error = %#v", client.hello)
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

func TestPresenterRequiresLifetimeCapability(t *testing.T) {
	h := startServerHarness(t, newPresenterClock())
	conn := dialSocket(t, h.server.SocketPath())
	defer conn.Close()
	hello := protocol.Hello{
		Major: protocol.ProtocolMajor, Minor: protocol.ProtocolMinor, Role: protocol.RolePresenter,
		Capabilities: []string{RequiredCapability},
	}
	if err := writeEnvelope(conn, protocol.KindHello, 0, 0, hello); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.ReadFrame(conn); err == nil {
		t.Fatal("presenter without lifetime capability was accepted")
	}
}

func TestProducerCapabilityAuthorizesKeyedCommands(t *testing.T) {
	h := startServerHarness(t, nil)
	client := connectPresenterWithHello(t, h.server.SocketPath(), helloWithProducer())
	defer client.conn.Close()
	if !hasCapability(client.hello.Capabilities, protocol.CapabilityBatteryProducer) {
		t.Fatalf("service capabilities = %#v", client.hello.Capabilities)
	}

	value := int32(15)
	command := protocol.Command{Kind: protocol.CommandProducerPublish, Producer: &protocol.ProducerRequest{
		Key: "sysc-shell:battery-low", AppName: "sysc-shell", Summary: "Battery low",
		Body: "Battery is at 15%.", Urgency: protocol.UrgencyCritical, Value: &value,
	}}
	if err := writeEnvelope(client.conn, protocol.KindCommand, 1, 0, command); err != nil {
		t.Fatal(err)
	}
	first := readReply(t, client.conn, 1)
	if !first.OK || first.ID == 0 || first.Replaced {
		t.Fatalf("first producer reply = %#v", first)
	}

	value = 10
	if err := writeEnvelope(client.conn, protocol.KindCommand, 2, 0, command); err != nil {
		t.Fatal(err)
	}
	replacement := readReply(t, client.conn, 2)
	if !replacement.OK || replacement.ID != first.ID || !replacement.Replaced {
		t.Fatalf("replacement producer reply = %#v", replacement)
	}

	if err := writeEnvelope(client.conn, protocol.KindCommand, 3, 0, protocol.Command{
		Kind:     protocol.CommandProducerClose,
		Producer: &protocol.ProducerRequest{Key: "sysc-shell:battery-low"},
	}); err != nil {
		t.Fatal(err)
	}
	closed := readReply(t, client.conn, 3)
	if !closed.OK || closed.ID != first.ID {
		t.Fatalf("producer close reply = %#v", closed)
	}
	if got := snapshotOwner(t, h.owner); len(got.Active) != 0 {
		t.Fatalf("producer close snapshot = %#v", got)
	}
}

func TestProducerCommandRequiresNegotiatedCapability(t *testing.T) {
	h := startServerHarness(t, nil)
	client := connectPresenter(t, h.server.SocketPath())
	defer client.conn.Close()
	if err := writeEnvelope(client.conn, protocol.KindCommand, 1, 0, protocol.Command{
		Kind:     protocol.CommandProducerPublish,
		Producer: &protocol.ProducerRequest{Key: "sysc-shell:battery-low", Summary: "Battery low"},
	}); err != nil {
		t.Fatal(err)
	}
	reply := readReply(t, client.conn, 1)
	if reply.OK || reply.Error == nil || reply.Error.Code != protocol.ErrorUnavailable {
		t.Fatalf("unauthorized producer reply = %#v", reply)
	}
	if got := snapshotOwner(t, h.owner); len(got.Active) != 0 {
		t.Fatalf("unauthorized producer mutated state = %#v", got)
	}
}

func TestPresentationRenewReturnsAuthoritativeLifetimes(t *testing.T) {
	clock := newPresenterClock()
	h := startServerHarness(t, clock)
	id := addCandidate(t, h.owner, "countdown", 10*time.Second)
	client := connectPresenter(t, h.server.SocketPath())
	defer client.conn.Close()

	clock.Advance(3 * time.Second)
	command := protocol.Command{
		Kind:          protocol.CommandPresentationRenew,
		Presentations: []protocol.Presentation{{ID: id, State: protocol.PresentationHovered}},
	}
	if err := writeEnvelope(client.conn, protocol.KindCommand, 1, 0, command); err != nil {
		t.Fatal(err)
	}
	reply := readReply(t, client.conn, 1)
	if !reply.OK || len(reply.Lifetimes) != 1 {
		t.Fatalf("presentation reply = %#v", reply)
	}
	got := reply.Lifetimes[0]
	if got.ID != id || got.DurationMS != 10_000 || got.RemainingMS != 7_000 || got.Running {
		t.Fatalf("presentation lifetime = %#v", got)
	}
}

func TestReplacedPresenterCannotRenewOrDismiss(t *testing.T) {
	clock := newPresenterClock()
	h := startServerHarness(t, clock)
	id := addCandidate(t, h.owner, "live", 10*time.Second)
	first := connectPresenter(t, h.server.SocketPath())
	renew := protocol.Command{
		Kind:          protocol.CommandPresentationRenew,
		Presentations: []protocol.Presentation{{ID: id, State: protocol.PresentationQueued}},
	}
	if err := writeEnvelope(first.conn, protocol.KindCommand, 1, 0, renew); err != nil {
		t.Fatal(err)
	}
	if reply := readReply(t, first.conn, 1); !reply.OK {
		t.Fatalf("first renew = %#v", reply)
	}

	second := connectPresenter(t, h.server.SocketPath())
	defer second.conn.Close()
	assertSocketClosed(t, first.conn)
	if err := writeEnvelope(second.conn, protocol.KindCommand, 1, 0, renew); err != nil {
		t.Fatal(err)
	}
	if reply := readReply(t, second.conn, 1); !reply.OK {
		t.Fatalf("replacement renew = %#v", reply)
	}

	staleDismiss := executeCommand(h.owner, 1, protocol.Command{Kind: protocol.CommandDismiss, ID: id})
	if staleDismiss.OK || staleDismiss.Error == nil || staleDismiss.Error.Code != protocol.ErrorStale {
		t.Fatalf("stale dismiss = %#v", staleDismiss)
	}
	staleRenew := executeCommand(h.owner, 1, protocol.Command{
		Kind:          protocol.CommandPresentationRenew,
		Presentations: []protocol.Presentation{{ID: id, State: protocol.PresentationVisible}},
	})
	if staleRenew.OK || staleRenew.Error == nil || staleRenew.Error.Code != protocol.ErrorStale {
		t.Fatalf("stale renew = %#v", staleRenew)
	}
	clock.Advance(10 * time.Second)
	if !activeIDs(snapshotOwner(t, h.owner), id) {
		t.Fatal("replaced presenter changed the live lease")
	}
	if err := writeEnvelope(second.conn, protocol.KindCommand, 2, 0, protocol.Command{Kind: protocol.CommandDismiss, ID: id}); err != nil {
		t.Fatal(err)
	}
	if reply := readReply(t, second.conn, 2); !reply.OK {
		t.Fatalf("live dismiss = %#v", reply)
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
	hello    protocol.Hello
	snapshot protocol.Snapshot
}

func connectPresenter(t *testing.T, path string) presenterClient {
	return connectPresenterWithHello(t, path, validHello())
}

func connectPresenterWithHello(t *testing.T, path string, clientHello protocol.Hello) presenterClient {
	t.Helper()
	conn := dialSocket(t, path)
	if err := writeEnvelope(conn, protocol.KindHello, 0, 0, clientHello); err != nil {
		t.Fatal(err)
	}
	serverHelloEnvelope := readEnvelope(t, conn)
	if serverHelloEnvelope.Kind != protocol.KindHello {
		t.Fatalf("first server message = %#v", serverHelloEnvelope)
	}
	var serviceHello protocol.Hello
	decodePayload(t, serverHelloEnvelope, &serviceHello)
	if err := serviceHello.Validate(protocol.RolePresenter); err != nil {
		t.Fatal(err)
	}
	if !hasCapability(serviceHello.Capabilities, RequiredLifetimeCapability) {
		t.Fatalf("service capabilities = %#v", serviceHello.Capabilities)
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
	return presenterClient{conn: conn, hello: serviceHello, snapshot: snapshot}
}

func validHello() protocol.Hello {
	return protocol.Hello{
		Major: protocol.ProtocolMajor, Minor: protocol.ProtocolMinor, Role: protocol.RolePresenter,
		Capabilities: []string{RequiredCapability, RequiredLifetimeCapability},
	}
}

func helloWithProducer() protocol.Hello {
	hello := validHello()
	hello.Capabilities = append(hello.Capabilities, protocol.CapabilityBatteryProducer)
	return hello
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
	if err := conn.SetReadDeadline(time.Now().Add(handshakeTimeout)); err != nil {
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

func snapshotOwner(t *testing.T, owner *state.Owner) protocol.Snapshot {
	t.Helper()
	snapshot, err := owner.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestSnapshotWithManyWireImagesStillHandshakes(t *testing.T) {
	h := startServerHarness(t, nil)
	const count = 16
	ids := make([]uint32, 0, count)
	for i := range count {
		result, err := h.owner.Do(context.Background(), state.Command{Kind: state.Add, Candidate: notify.Candidate{
			Summary: "image", Urgency: protocol.UrgencyNormal,
			Image: &protocol.Image{
				MediaType: "image/png", Width: protocol.MaxWireImageLongEdge, Height: protocol.MaxWireImageLongEdge,
				Data: make([]byte, protocol.MaxWireImageBytes-i),
			},
		}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, result.ID)
	}
	client := connectPresenter(t, h.server.SocketPath())
	defer client.conn.Close()
	if len(client.snapshot.Active) != count {
		t.Fatalf("snapshot holds %d active, want %d", len(client.snapshot.Active), count)
	}
	for i, notification := range client.snapshot.Active {
		if notification.ID != ids[i] {
			t.Fatalf("active[%d] = %d, want %d", i, notification.ID, ids[i])
		}
	}
	if newest := client.snapshot.Active[count-1]; newest.Image == nil {
		t.Fatal("snapshot dropped the newest image")
	}
	if oldest := client.snapshot.Active[0]; oldest.Image != nil {
		t.Fatal("snapshot kept the oldest image over the frame limit")
	}
}
