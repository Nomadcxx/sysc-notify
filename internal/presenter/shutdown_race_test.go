package presenter

import (
	"sync"
	"testing"
	"time"

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
