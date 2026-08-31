package fdo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/godbus/dbus/v5"

	"github.com/Nomadcxx/sysc-notify/internal/state"
	"github.com/Nomadcxx/sysc-notify/protocol"
)

func TestServerOwnsNameOnceAndReportsLoss(t *testing.T) {
	requireSessionBus(t)
	firstConn, err := dbus.ConnectSessionBus()
	if err != nil {
		t.Fatal(err)
	}
	first := New(firstConn)
	firstOwner := state.Start(nil, first)
	if err := first.Serve(firstOwner); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = first.Close()
		_ = firstOwner.Close()
		_ = firstConn.Close()
	})

	secondConn, err := dbus.ConnectSessionBus()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondConn.Close() })
	second := New(secondConn)
	secondOwner := state.Start(nil, second)
	t.Cleanup(func() { _ = secondOwner.Close() })
	if err := second.Serve(secondOwner); err == nil {
		t.Fatal("second server acquired notification name")
	}

	if _, err := firstConn.ReleaseName(BusName); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-first.Done():
		if err == nil {
			t.Fatal("name loss reported no error")
		}
	case <-time.After(time.Second):
		t.Fatal("server did not report D-Bus name loss")
	}
}

func TestServerMetadataAndNotifyReplacement(t *testing.T) {
	h := startHarness(t, nil)
	var name, vendor, version, spec string
	if err := h.object.Call(Interface+".GetServerInformation", 0).Store(&name, &vendor, &version, &spec); err != nil {
		t.Fatal(err)
	}
	if name != ServerName || vendor != ServerVendor || version != ServerVersion || spec != SpecVersion {
		t.Fatalf("server information = %q, %q, %q, %q", name, vendor, version, spec)
	}
	var capabilities []string
	if err := h.object.Call(Interface+".GetCapabilities", 0).Store(&capabilities); err != nil {
		t.Fatal(err)
	}
	if len(capabilities) != 0 {
		t.Fatalf("unqualified capabilities = %v", capabilities)
	}

	first := sendNotify(t, h.object, 0, "first", nil, -1)
	replaced := sendNotify(t, h.object, first, "replacement", nil, 0)
	if replaced != first {
		t.Fatalf("replacement ID = %d, want %d", replaced, first)
	}
	missingReplacement := sendNotify(t, h.object, 999, "new", nil, 0)
	if missingReplacement == 0 || missingReplacement == 999 {
		t.Fatalf("missing replacement allocated %d", missingReplacement)
	}
	snapshot, err := h.owner.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Active) != 2 || snapshot.Active[0].Summary != "replacement" || snapshot.Active[1].Summary != "new" {
		t.Fatalf("active snapshot = %#v", snapshot.Active)
	}
}

func TestMalformedNotifyAndMissingClosePreserveState(t *testing.T) {
	h := startHarness(t, nil)
	id := sendNotify(t, h.object, 0, "valid", nil, 0)
	call := h.object.Call(Interface+".Notify", 0,
		"app", uint32(0), "", "invalid", "", []string{},
		map[string]dbus.Variant{"urgency": dbus.MakeVariant("critical")}, int32(0),
	)
	assertDBusError(t, call.Err, dbusInvalidArgs)
	assertDBusError(t, h.object.Call(Interface+".CloseNotification", 0, uint32(999)).Err, invalidNotification)
	snapshot, err := h.owner.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Active) != 1 || snapshot.Active[0].ID != id || snapshot.Active[0].Summary != "valid" {
		t.Fatalf("state changed after invalid calls: %#v", snapshot.Active)
	}
}

func TestServerConvertsImageDataOverDBus(t *testing.T) {
	h := startHarness(t, nil)
	pixels := []byte{
		0xff, 0x00, 0x00, 0xff,
		0x00, 0xff, 0x00, 0xff,
	}
	id := sendNotify(t, h.object, 0, "image", map[string]dbus.Variant{
		"image-data": dbus.MakeVariant(imageData{
			Width: 2, Height: 1, RowStride: 8, HasAlpha: true, BitsPerSample: 8, Channels: 4, Data: pixels,
		}),
	}, 0)
	snapshot, err := h.owner.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Active) != 1 || snapshot.Active[0].ID != id || snapshot.Active[0].Image == nil ||
		snapshot.Active[0].Image.Width != 2 || snapshot.Active[0].Image.Height != 1 {
		t.Fatalf("converted image snapshot = %#v", snapshot.Active)
	}
}

func TestServerCapturesSenderLineage(t *testing.T) {
	root := t.TempDir()
	pid := uint32(os.Getpid())
	writeProcessStat(t, root, pid, 1, 30)
	writeProcessStat(t, root, 1, 0, 10)
	h := startHarnessAt(t, nil, root)

	id := sendNotify(t, h.object, 0, "lineage", nil, 0)
	snapshot, err := h.owner.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []protocol.Process{{PID: pid, StartTime: 30}, {PID: 1, StartTime: 10}}
	if len(snapshot.Active) != 1 || snapshot.Active[0].ID != id || !reflect.DeepEqual(snapshot.Active[0].SenderLineage, want) {
		t.Fatalf("sender lineage = %#v, want %#v", snapshot.Active, want)
	}
}

func TestServerEmitsCloseReasons(t *testing.T) {
	h := startHarness(t, nil)
	signals := watchSignals(t, h.client)

	requested := sendNotify(t, h.object, 0, "requested", nil, 0)
	if err := h.object.Call(Interface+".CloseNotification", 0, requested).Err; err != nil {
		t.Fatal(err)
	}
	assertSignal(t, signals, "NotificationClosed", requested, uint32(protocol.CloseRequested))

	dismissed := sendNotify(t, h.object, 0, "dismissed", nil, 0)
	if _, err := h.owner.Do(context.Background(), state.Command{Kind: state.Dismiss, ID: dismissed}); err != nil {
		t.Fatal(err)
	}
	assertSignal(t, signals, "NotificationClosed", dismissed, uint32(protocol.CloseDismissed))

	expired := sendNotify(t, h.object, 0, "expired", nil, 10)
	assertSignal(t, signals, "NotificationClosed", expired, uint32(protocol.CloseExpired))

	oldest := sendNotify(t, h.object, 0, "oldest", nil, 0)
	for range protocol.MaxActiveNotifications {
		sendNotify(t, h.object, 0, "fill", nil, 0)
	}
	assertSignal(t, signals, "NotificationClosed", oldest, uint32(protocol.CloseUndefined))
}

func TestServerEmitsActionAndReplyBeforeClose(t *testing.T) {
	h := startHarness(t, nil)
	signals := watchSignals(t, h.client)

	var actionID uint32
	if err := h.object.Call(Interface+".Notify", 0,
		"app", uint32(0), "", "action", "", []string{"open", "Open"}, map[string]dbus.Variant{}, int32(0),
	).Store(&actionID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.owner.Do(context.Background(), state.Command{Kind: state.InvokeAction, ID: actionID, ActionKey: "open"}); err != nil {
		t.Fatal(err)
	}
	assertSignal(t, signals, "ActionInvoked", actionID, "open")
	assertSignal(t, signals, "NotificationClosed", actionID, uint32(protocol.CloseDismissed))

	replyID := sendNotify(t, h.object, 0, "reply", map[string]dbus.Variant{
		"x-kde-reply-placeholder-text": dbus.MakeVariant("Reply"),
	}, 0)
	if _, err := h.owner.Do(context.Background(), state.Command{Kind: state.SubmitReply, ID: replyID, ReplyText: "answer"}); err != nil {
		t.Fatal(err)
	}
	assertSignal(t, signals, "NotificationReplied", replyID, "answer")
	assertSignal(t, signals, "NotificationClosed", replyID, uint32(protocol.CloseDismissed))
}

type harness struct {
	server *Server
	owner  *state.Owner
	client *dbus.Conn
	object dbus.BusObject
}

func startHarness(t *testing.T, clock state.Clock) harness {
	return startHarnessAt(t, clock, t.TempDir())
}

func startHarnessAt(t *testing.T, clock state.Clock, procRoot string) harness {
	t.Helper()
	requireSessionBus(t)
	serverConn, err := dbus.ConnectSessionBus()
	if err != nil {
		t.Fatal(err)
	}
	server := NewAt(serverConn, procRoot)
	owner := state.Start(clock, server)
	if err := server.Serve(owner); err != nil {
		_ = owner.Close()
		_ = serverConn.Close()
		t.Fatal(err)
	}
	client, err := dbus.ConnectSessionBus()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		_ = owner.Close()
		_ = serverConn.Close()
	})
	return harness{server: server, owner: owner, client: client, object: client.Object(BusName, ObjectPath)}
}

func writeProcessStat(t *testing.T, root string, pid, parent uint32, start uint64) {
	t.Helper()
	dir := filepath.Join(root, strconv.FormatUint(uint64(pid), 10))
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	fields := make([]string, 20)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = "S"
	fields[1] = strconv.FormatUint(uint64(parent), 10)
	fields[19] = strconv.FormatUint(start, 10)
	contents := fmt.Sprintf("%d (dbus client) %s\n", pid, strings.Join(fields, " "))
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func sendNotify(t *testing.T, object dbus.BusObject, replacesID uint32, summary string, hints map[string]dbus.Variant, timeout int32) uint32 {
	t.Helper()
	if hints == nil {
		hints = map[string]dbus.Variant{}
	}
	var id uint32
	if err := object.Call(Interface+".Notify", 0, "app", replacesID, "", summary, "body", []string{}, hints, timeout).Store(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func assertDBusError(t *testing.T, err error, name string) {
	t.Helper()
	if err == nil {
		t.Fatalf("call succeeded, want %s", name)
	}
	var busError dbus.Error
	if !errors.As(err, &busError) || busError.Name != name {
		t.Fatalf("D-Bus error = %#v, want %s", err, name)
	}
}

func watchSignals(t *testing.T, conn *dbus.Conn) chan *dbus.Signal {
	t.Helper()
	if err := conn.AddMatchSignal(dbus.WithMatchObjectPath(ObjectPath), dbus.WithMatchInterface(Interface)); err != nil {
		t.Fatal(err)
	}
	signals := make(chan *dbus.Signal, 256)
	conn.Signal(signals)
	t.Cleanup(func() {
		conn.RemoveSignal(signals)
		_ = conn.RemoveMatchSignal(dbus.WithMatchObjectPath(ObjectPath), dbus.WithMatchInterface(Interface))
	})
	return signals
}

func assertSignal(t *testing.T, signals <-chan *dbus.Signal, member string, body ...any) {
	t.Helper()
	select {
	case signal := <-signals:
		if signal == nil || signal.Name != Interface+"."+member {
			t.Fatalf("signal = %#v, want %s.%s", signal, Interface, member)
		}
		if len(signal.Body) != len(body) {
			t.Fatalf("signal body = %#v, want %#v", signal.Body, body)
		}
		for i := range body {
			if signal.Body[i] != body[i] {
				t.Fatalf("signal body = %#v, want %#v", signal.Body, body)
			}
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s.%s", Interface, member)
	}
}

func requireSessionBus(t *testing.T) {
	t.Helper()
	if address := os.Getenv("DBUS_SESSION_BUS_ADDRESS"); !strings.Contains(address, "/tmp/") {
		t.Skip("requires dbus-run-session")
	}
}
