package integration

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/godbus/dbus/v5"

	"github.com/Nomadcxx/sysc-notify/internal/fdo"
	"github.com/Nomadcxx/sysc-notify/protocol"
)

var daemonBinary string

func TestMain(m *testing.M) {
	if !strings.Contains(os.Getenv("DBUS_SESSION_BUS_ADDRESS"), "/tmp/") {
		os.Exit(m.Run())
	}
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	temporary, err := os.MkdirTemp("", "sysc-notify-integration-")
	if err != nil {
		panic(err)
	}
	daemonBinary = filepath.Join(temporary, "sysc-notify")
	build := exec.Command("go", "build", "-o", daemonBinary, "./cmd/sysc-notify")
	build.Dir = root
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		_ = os.RemoveAll(temporary)
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(temporary)
	os.Exit(code)
}

func TestDaemonDBusReplacementExpiryAndShutdown(t *testing.T) {
	requireSessionBus(t)
	d := startDaemon(t, "", "")
	client := connectBus(t)
	signals := watchSignals(t, client)

	id := notify(t, client, 0, "first", "body", nil, 0)
	if replaced := notify(t, client, id, "replacement", "body", nil, 0); replaced != id {
		t.Fatalf("replacement ID = %d, want %d", replaced, id)
	}
	if err := client.Object(fdo.BusName, fdo.ObjectPath).Call(fdo.Interface+".CloseNotification", 0, id).Err; err != nil {
		t.Fatal(err)
	}
	assertClosedSignal(t, signals, id, protocol.CloseRequested)

	expiring := notify(t, client, 0, "expiring", "body", nil, 20)
	assertClosedSignal(t, signals, expiring, protocol.CloseExpired)
	d.stop(t)

	probe := connectBus(t)
	reply, err := probe.RequestName(fdo.BusName, dbus.NameFlagDoNotQueue)
	if err != nil || reply != dbus.RequestNameReplyPrimaryOwner {
		t.Fatalf("notification name after shutdown = %v, %v", reply, err)
	}
}

func TestDaemonQuarantinesCorruptHistory(t *testing.T) {
	requireSessionBus(t)
	stateHome := t.TempDir()
	dir := filepath.Join(stateHome, "sysc-notify")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "history.json"), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	d := startDaemon(t, "", stateHome)
	d.stop(t)
	matches, err := filepath.Glob(filepath.Join(dir, "history.quarantine-*.json"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("quarantined history = %v, %v", matches, err)
	}
}

type daemon struct {
	cmd        *exec.Cmd
	runtimeDir string
	stateHome  string
	stderr     bytes.Buffer
	wait       chan error
	stopOnce   sync.Once
	stopErr    error
}

func startDaemon(t *testing.T, runtimeDir, stateHome string) *daemon {
	t.Helper()
	if runtimeDir == "" {
		runtimeDir = privateRuntimeDir(t)
	}
	if stateHome == "" {
		stateHome = t.TempDir()
	}
	d := &daemon{runtimeDir: runtimeDir, stateHome: stateHome, wait: make(chan error, 1)}
	d.cmd = exec.Command(daemonBinary)
	d.cmd.Env = append(os.Environ(), "XDG_RUNTIME_DIR="+runtimeDir, "XDG_STATE_HOME="+stateHome)
	d.cmd.Stderr = &d.stderr
	if err := d.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { d.wait <- d.cmd.Wait() }()
	t.Cleanup(func() { d.stop(t) })

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-d.wait:
			t.Fatalf("daemon stopped during startup: %v\n%s", err, d.stderr.String())
		default:
		}
		conn, err := dbus.ConnectSessionBus()
		if err == nil {
			var owned bool
			err = conn.BusObject().Call("org.freedesktop.DBus.NameHasOwner", 0, fdo.BusName).Store(&owned)
			_ = conn.Close()
			if err == nil && owned {
				return d
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = d.cmd.Process.Kill()
	t.Fatalf("daemon did not become ready\n%s", d.stderr.String())
	return nil
}

func (d *daemon) stop(t *testing.T) {
	t.Helper()
	d.stopOnce.Do(func() {
		if d.cmd.Process == nil {
			return
		}
		if err := d.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			d.stopErr = err
			return
		}
		select {
		case d.stopErr = <-d.wait:
		case <-time.After(3 * time.Second):
			_ = d.cmd.Process.Kill()
			d.stopErr = fmt.Errorf("daemon did not stop\n%s", d.stderr.String())
		}
	})
	if d.stopErr != nil {
		t.Fatalf("stop daemon: %v\n%s", d.stopErr, d.stderr.String())
	}
}

func connectBus(t *testing.T) *dbus.Conn {
	t.Helper()
	conn, err := dbus.ConnectSessionBus()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func notify(t *testing.T, conn *dbus.Conn, replacesID uint32, summary, body string, hints map[string]dbus.Variant, timeout int32) uint32 {
	t.Helper()
	if hints == nil {
		hints = map[string]dbus.Variant{}
	}
	var id uint32
	err := conn.Object(fdo.BusName, fdo.ObjectPath).Call(fdo.Interface+".Notify", 0,
		"integration", replacesID, "", summary, body, []string{}, hints, timeout,
	).Store(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func watchSignals(t *testing.T, conn *dbus.Conn) chan *dbus.Signal {
	t.Helper()
	if err := conn.AddMatchSignal(dbus.WithMatchObjectPath(fdo.ObjectPath), dbus.WithMatchInterface(fdo.Interface)); err != nil {
		t.Fatal(err)
	}
	signals := make(chan *dbus.Signal, 32)
	conn.Signal(signals)
	t.Cleanup(func() { conn.RemoveSignal(signals) })
	return signals
}

func assertClosedSignal(t *testing.T, signals <-chan *dbus.Signal, id uint32, reason protocol.CloseReason) {
	t.Helper()
	select {
	case signal := <-signals:
		if signal == nil || signal.Name != fdo.Interface+".NotificationClosed" || len(signal.Body) != 2 ||
			signal.Body[0] != id || signal.Body[1] != uint32(reason) {
			t.Fatalf("close signal = %#v, want %d/%d", signal, id, reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for close signal %d/%d", id, reason)
	}
}

func privateRuntimeDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sysc-notify-integration-runtime-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func requireSessionBus(t *testing.T) {
	t.Helper()
	if !strings.Contains(os.Getenv("DBUS_SESSION_BUS_ADDRESS"), "/tmp/") {
		t.Skip("requires dbus-run-session")
	}
}
