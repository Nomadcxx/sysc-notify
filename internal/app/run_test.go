package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/godbus/dbus/v5"

	"github.com/Nomadcxx/sysc-notify/internal/fdo"
)

func TestRunServesUntilCancellationAndCleansUp(t *testing.T) {
	requireSessionBus(t)
	runtimeDir := privateRuntimeDir(t)
	stateHome := t.TempDir()
	procRoot := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{
			RuntimeDir: runtimeDir,
			StateHome:  stateHome,
			ProcRoot:   procRoot,
			Ready:      ready,
		})
	}()

	waitReady(t, ready, done)
	socket := filepath.Join(runtimeDir, "sysc-notify", "presenter.v1.sock")
	if info, err := os.Lstat(socket); err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("presenter socket = %#v, %v", info, err)
	}
	conn, err := dbus.ConnectSessionBus()
	if err != nil {
		t.Fatal(err)
	}
	var name, vendor, version, spec string
	if err := conn.Object(fdo.BusName, fdo.ObjectPath).Call(fdo.Interface+".GetServerInformation", 0).Store(&name, &vendor, &version, &spec); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	_ = conn.Close()
	if name != fdo.ServerName {
		t.Fatalf("server name = %q", name)
	}

	cancel()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if _, err := os.Lstat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket remained after shutdown: %v", err)
	}
}

func TestRunUnwindsPresenterWhenBusNameIsTaken(t *testing.T) {
	requireSessionBus(t)
	blocker, err := dbus.ConnectSessionBus()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Close() })
	if reply, err := blocker.RequestName(fdo.BusName, dbus.NameFlagDoNotQueue); err != nil || reply != dbus.RequestNameReplyPrimaryOwner {
		t.Fatalf("occupy notification name = %v, %v", reply, err)
	}

	runtimeDir := privateRuntimeDir(t)
	ready := make(chan struct{})
	err = Run(context.Background(), Config{
		RuntimeDir: runtimeDir,
		StateHome:  t.TempDir(),
		ProcRoot:   t.TempDir(),
		Ready:      ready,
	})
	if err == nil || !strings.Contains(err.Error(), "acquire "+fdo.BusName) {
		t.Fatalf("Run() = %v, want name acquisition error", err)
	}
	select {
	case <-ready:
		t.Fatal("failed startup published readiness")
	default:
	}
	socket := filepath.Join(runtimeDir, "sysc-notify", "presenter.v1.sock")
	if _, err := os.Lstat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket remained after startup unwind: %v", err)
	}
}

func waitReady(t *testing.T, ready <-chan struct{}, done <-chan error) {
	t.Helper()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("Run() stopped before readiness: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not become ready")
	}
}

func waitDone(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not stop")
		return nil
	}
}

func privateRuntimeDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sysc-notify-runtime-")
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
	if address := os.Getenv("DBUS_SESSION_BUS_ADDRESS"); !strings.Contains(address, "/tmp/") {
		t.Skip("requires dbus-run-session")
	}
}
