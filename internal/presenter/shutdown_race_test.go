package presenter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Nomadcxx/sysc-notify/internal/notify"
	"github.com/Nomadcxx/sysc-notify/internal/state"
	"github.com/Nomadcxx/sysc-notify/protocol"
)

// TestResyncGoroutineIsTrackedAtShutdown is a race probe for the resync
// goroutine lifecycle: Publish may spawn a tracked goroutine that calls
// Owner.Do, and Close waits on the same WaitGroup. wg.Add must not race
// wg.Wait, and a resync requested during shutdown must be dropped rather than
// spawned. Run under -race.
func TestResyncGoroutineIsTrackedAtShutdown(t *testing.T) {
	for range 30 {
		h := startServerHarness(t, nil)
		server := h.server

		// Force a live connection whose queue is already at its bound, so the
		// next delta arms a resync and Publish spawns the tracked goroutine.
		conn := activeConnection(1)
		for sequence := uint64(1); sequence <= protocol.MaxPresenterQueueMessages; sequence++ {
			conn.enqueue(outbound{data: []byte{1}, sequence: sequence})
		}
		server.mu.Lock()
		server.current = conn
		server.mu.Unlock()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sequence := uint64(protocol.MaxPresenterQueueMessages + 1); sequence <= protocol.MaxPresenterQueueMessages+50; sequence++ {
				server.Publish(state.Event{Sequence: sequence, Delta: &protocol.Delta{Kind: protocol.DeltaReplaced, ID: 1}})
			}
		}()
		// Close concurrently with the publishing burst.
		go func() {
			time.Sleep(time.Millisecond)
			_ = server.Close()
		}()
		wg.Wait()
		_ = server.Close()
	}
}

// A connection that is accepted but still reading its hello is in neither
// s.current nor s.preparing. Close must track it explicitly: otherwise
// wg.Wait blocks on handle until the handshake deadline expires (5s), and a
// handshake that completes after Close started promotes a connection that
// nothing will ever fail.
func TestCloseDuringHandshakeDoesNotPromote(t *testing.T) {
	h := startServerHarness(t, nil)
	conn := dialSocket(t, h.server.SocketPath())
	defer conn.Close()
	time.Sleep(50 * time.Millisecond) // accepted; handle is blocked in readHello
	closed := make(chan error, 1)
	go func() { closed <- h.server.Close() }()
	time.Sleep(100 * time.Millisecond) // Close has already failed the connection
	// The write may report EPIPE because Close closed the socket; that is the
	// point. What matters is that no handshake frame comes back.
	_ = writeEnvelope(conn, protocol.KindHello, 0, 0, validHello())
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.ReadFrame(conn); err == nil {
		t.Fatal("presenter was served hello/snapshot after Close started")
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close still blocked while the presenter stayed connected")
	}
}

// A peer that connects and never sends a hello must not hold Close for the
// whole handshake timeout: Close closes the socket, readHello returns EOF.
func TestCloseUnblocksPendingHello(t *testing.T) {
	h := startServerHarness(t, nil)
	conn := dialSocket(t, h.server.SocketPath())
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	if err := h.server.Close(); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(start); waited > handshakeTimeout/2 {
		t.Fatalf("Close waited %s on a connection stuck in readHello", waited)
	}
}

// gatedSink pins the owner goroutine inside Publish so a presenter handshake
// can be held between readHello and the promotion.
type gatedSink struct {
	inner   state.Sink
	release chan struct{}
	held    chan struct{}
	once    sync.Once
}

func (s *gatedSink) Publish(event state.Event) bool {
	s.once.Do(func() { close(s.held) })
	<-s.release
	return s.inner.Publish(event)
}

func TestCloseWhileSnapshotPending(t *testing.T) {
	runtimeDir := privateTempDir(t)
	server := NewAt(runtimeDir)
	gate := &gatedSink{inner: server, release: make(chan struct{}), held: make(chan struct{})}
	owner := state.Start(nil, gate)
	if err := server.Serve(owner); err != nil {
		_ = owner.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Close()
		_ = owner.Close()
	})

	added := make(chan struct{})
	go func() {
		defer close(added)
		_, _ = owner.Do(context.Background(), state.Command{Kind: state.Add, Candidate: notify.Candidate{Summary: "busy"}})
	}()
	<-gate.held // the owner goroutine is now parked inside Publish

	conn := dialSocket(t, server.SocketPath())
	defer conn.Close()
	if err := writeEnvelope(conn, protocol.KindHello, 0, 0, validHello()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		server.mu.Lock()
		claimed := server.preparing != nil
		server.mu.Unlock()
		if claimed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("handle never claimed s.preparing")
		}
		time.Sleep(time.Millisecond)
	}

	closed := make(chan error, 1)
	go func() { closed <- server.Close() }()
	// Close fails the socket even though handle is parked in owner.Snapshot.
	select {
	case <-closed:
		t.Fatal("Close returned while handle was still waiting for the owner")
	case <-time.After(100 * time.Millisecond):
	}
	close(gate.release)
	<-added
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close blocked after the owner became available")
	}
	server.mu.Lock()
	promoted := server.current != nil
	server.mu.Unlock()
	if promoted {
		t.Fatal("connection became current after Close started")
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.ReadFrame(conn); err == nil {
		t.Fatal("handshake frames were written after Close started")
	}
}
