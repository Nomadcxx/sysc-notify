// Package dbustest gives each test its own session bus.
//
// The notification service name is global to a bus, so tests that acquire it
// cannot share one. Go runs package tests in parallel processes, which made a single
// shared bus a source of cross-package interference. Each test gets a private
// daemon instead, and its address is exported so code under test that calls
// dbus.ConnectSessionBus reaches the same bus.
package dbustest

import (
	"bufio"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/godbus/dbus/v5"
)

var (
	mu       sync.Mutex
	sessions = map[testing.TB]string{}
)

// Session starts a private session bus for this test, or returns the one it
// already has. The daemon is stopped when the test finishes.
func Session(t testing.TB) string {
	t.Helper()
	mu.Lock()
	if address, ok := sessions[t]; ok {
		mu.Unlock()
		return address
	}
	mu.Unlock()

	if _, err := exec.LookPath("dbus-daemon"); err != nil {
		t.Skip("dbus-daemon is not installed")
	}
	command := exec.Command("dbus-daemon", "--session", "--nofork", "--print-address=1")
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(output).ReadString('\n')
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("private bus did not report an address: %v", err)
	}
	address := strings.TrimSpace(line)

	mu.Lock()
	sessions[t] = address
	mu.Unlock()
	t.Setenv("DBUS_SESSION_BUS_ADDRESS", address)
	t.Cleanup(func() {
		mu.Lock()
		delete(sessions, t)
		mu.Unlock()
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	return address
}

// Connect dials this test's private bus.
func Connect(t testing.TB) *dbus.Conn {
	t.Helper()
	conn, err := dbus.Connect(Session(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
