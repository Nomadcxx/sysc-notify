package integration

import (
	"encoding/json"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/godbus/dbus/v5"

	"github.com/Nomadcxx/sysc-notify/internal/fdo"
	"github.com/Nomadcxx/sysc-notify/internal/presenter"
	"github.com/Nomadcxx/sysc-notify/protocol"
)

func TestPresenterActionsAndInlineReply(t *testing.T) {
	requireSessionBus(t)
	d := startDaemon(t, "", "")
	client := connectBus(t)
	signals := watchSignals(t, client)
	p := connectPresenter(t, d.runtimeDir)
	defer p.Close()

	var actionID uint32
	err := client.Object(fdo.BusName, fdo.ObjectPath).Call(fdo.Interface+".Notify", 0,
		"integration", uint32(0), "", "action", "body", []string{"open", "Open"}, map[string]dbus.Variant{}, int32(0),
	).Store(&actionID)
	if err != nil {
		t.Fatal(err)
	}
	readDelta(t, p, protocol.DeltaAdded, actionID)
	sendCommand(t, p, 1, protocol.Command{Kind: protocol.CommandAction, ID: actionID, ActionKey: "open"})
	readReply(t, p, 1)
	assertSignal(t, signals, "ActionInvoked", actionID, "open")
	assertSignal(t, signals, "NotificationClosed", actionID, uint32(protocol.CloseDismissed))

	replyID := notify(t, client, 0, "reply", "body", map[string]dbus.Variant{
		"x-kde-reply-placeholder-text": dbus.MakeVariant("Reply"),
	}, 0)
	readDelta(t, p, protocol.DeltaAdded, replyID)
	sendCommand(t, p, 2, protocol.Command{Kind: protocol.CommandReply, ID: replyID, Text: "answer"})
	readReply(t, p, 2)
	assertSignal(t, signals, "NotificationReplied", replyID, "answer")
	assertSignal(t, signals, "NotificationClosed", replyID, uint32(protocol.CloseDismissed))
}

func TestPresenterReconnectAndServiceRestartRestoreOnlyPublicHistory(t *testing.T) {
	requireSessionBus(t)
	runtimeDir := privateRuntimeDir(t)
	stateHome := t.TempDir()
	d := startDaemon(t, runtimeDir, stateHome)
	client := connectBus(t)

	publicID := notify(t, client, 0, "public", "body", nil, 0)
	privateID := notify(t, client, 0, "private", "body", map[string]dbus.Variant{
		"x-sysc-private": dbus.MakeVariant(true),
	}, 0)
	for _, id := range []uint32{publicID, privateID} {
		if err := client.Object(fdo.BusName, fdo.ObjectPath).Call(fdo.Interface+".CloseNotification", 0, id).Err; err != nil {
			t.Fatal(err)
		}
	}

	first, snapshot := connectPresenterSnapshot(t, runtimeDir)
	if len(snapshot.Active) != 0 || !historyHas(snapshot, publicID) || historyHas(snapshot, privateID) {
		t.Fatalf("first snapshot = %#v", snapshot)
	}
	_ = first.Close()

	activeID := notify(t, client, 0, "active", "body", nil, 0)
	second, snapshot := connectPresenterSnapshot(t, runtimeDir)
	if !activeHas(snapshot, activeID) || !historyHas(snapshot, publicID) {
		t.Fatalf("reconnect snapshot = %#v", snapshot)
	}
	_ = second.Close()
	d.stop(t)

	d = startDaemon(t, runtimeDir, stateHome)
	third, snapshot := connectPresenterSnapshot(t, runtimeDir)
	defer third.Close()
	if len(snapshot.Active) != 0 || !historyHas(snapshot, publicID) || historyHas(snapshot, privateID) {
		t.Fatalf("restart snapshot = %#v", snapshot)
	}
}

func TestSlowPresenterIsDroppedWithoutBlockingDBus(t *testing.T) {
	requireSessionBus(t)
	d := startDaemon(t, "", "")
	client := connectBus(t)
	slow := dialPresenter(t, d.runtimeDir)
	defer slow.Close()
	if err := writeEnvelope(slow, protocol.KindHello, 0, 0, protocol.Hello{
		Major: protocol.ProtocolMajor, Minor: protocol.ProtocolMinor, Role: protocol.RolePresenter,
		Capabilities: []string{presenter.RequiredCapability},
	}); err != nil {
		t.Fatal(err)
	}

	body := strings.Repeat("x", protocol.MaxBodyBytes)
	id := notify(t, client, 0, "slow", body, nil, 0)
	for range 400 {
		if replaced := notify(t, client, id, "slow", body, nil, 0); replaced != id {
			t.Fatalf("replacement ID = %d, want %d", replaced, id)
		}
	}
	if got := notify(t, client, 0, "still responsive", "body", nil, 0); got == 0 {
		t.Fatal("D-Bus stopped responding after slow presenter")
	}

	if err := slow.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for range 500 {
		if _, err := protocol.ReadFrame(slow); err != nil {
			return
		}
	}
	t.Fatal("slow presenter remained connected")
}

func connectPresenter(t *testing.T, runtimeDir string) *net.UnixConn {
	t.Helper()
	conn, _ := connectPresenterSnapshot(t, runtimeDir)
	return conn
}

func connectPresenterSnapshot(t *testing.T, runtimeDir string) (*net.UnixConn, protocol.Snapshot) {
	t.Helper()
	conn := dialPresenter(t, runtimeDir)
	if err := writeEnvelope(conn, protocol.KindHello, 0, 0, protocol.Hello{
		Major: protocol.ProtocolMajor, Minor: protocol.ProtocolMinor, Role: protocol.RolePresenter,
		Capabilities: []string{presenter.RequiredCapability},
	}); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	if envelope := readEnvelope(t, conn); envelope.Kind != protocol.KindHello {
		t.Fatalf("first presenter frame = %#v", envelope)
	}
	envelope := readEnvelope(t, conn)
	if envelope.Kind != protocol.KindSnapshot {
		t.Fatalf("second presenter frame = %#v", envelope)
	}
	var snapshot protocol.Snapshot
	decodePayload(t, envelope, &snapshot)
	return conn, snapshot
}

func dialPresenter(t *testing.T, runtimeDir string) *net.UnixConn {
	t.Helper()
	path := filepath.Join(runtimeDir, "sysc-notify", "presenter.v1.sock")
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func sendCommand(t *testing.T, conn net.Conn, requestID uint64, command protocol.Command) {
	t.Helper()
	if err := writeEnvelope(conn, protocol.KindCommand, requestID, 0, command); err != nil {
		t.Fatal(err)
	}
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
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
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
	return envelope
}

func decodePayload(t *testing.T, envelope protocol.Envelope, destination any) {
	t.Helper()
	if err := protocol.DecodeStrict(envelope.Payload, destination); err != nil {
		t.Fatal(err)
	}
}

func readDelta(t *testing.T, conn net.Conn, kind protocol.DeltaKind, id uint32) {
	t.Helper()
	for {
		envelope := readEnvelope(t, conn)
		if envelope.Kind != string(kind) {
			continue
		}
		var delta protocol.Delta
		decodePayload(t, envelope, &delta)
		gotID := delta.ID
		if delta.Notification != nil {
			gotID = delta.Notification.ID
		}
		if delta.Kind != kind || gotID != id {
			t.Fatalf("delta = %#v, want ID %d", delta, id)
		}
		return
	}
}

func readReply(t *testing.T, conn net.Conn, requestID uint64) {
	t.Helper()
	for {
		envelope := readEnvelope(t, conn)
		if envelope.Kind != protocol.KindReply {
			continue
		}
		var reply protocol.Reply
		decodePayload(t, envelope, &reply)
		if envelope.RequestID != requestID || !reply.OK || reply.Error != nil {
			t.Fatalf("reply = %#v/%#v", envelope, reply)
		}
		return
	}
}

func assertSignal(t *testing.T, signals <-chan *dbus.Signal, member string, body ...any) {
	t.Helper()
	select {
	case signal := <-signals:
		if signal == nil || signal.Name != fdo.Interface+"."+member || len(signal.Body) != len(body) {
			t.Fatalf("signal = %#v, want %s %#v", signal, member, body)
		}
		for i := range body {
			if signal.Body[i] != body[i] {
				t.Fatalf("signal body = %#v, want %#v", signal.Body, body)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", member)
	}
}

func activeHas(snapshot protocol.Snapshot, id uint32) bool {
	for _, notification := range snapshot.Active {
		if notification.ID == id {
			return true
		}
	}
	return false
}

func historyHas(snapshot protocol.Snapshot, id uint32) bool {
	for _, entry := range snapshot.History {
		if entry.ID == id {
			return true
		}
	}
	return false
}
