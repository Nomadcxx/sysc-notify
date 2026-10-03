package presenter

import (
	"testing"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

func activeConnection(generation uint64) *connection {
	c := newConnection(nil, generation)
	c.active = true
	return c
}

// When the bounded queue fills, a delta must arm a resync (reported exactly
// once) instead of tearing the connection down, and the connection must stay
// open while the snapshot is pending.
func TestOverflowArmsResyncInsteadOfClosing(t *testing.T) {
	c := activeConnection(1)
	resyncRequests := 0
	for sequence := uint64(1); sequence <= protocol.MaxPresenterQueueMessages; sequence++ {
		published, resync := c.enqueueDelta(outbound{data: []byte{1}, sequence: sequence})
		if !published {
			t.Fatalf("delta %d: connection closed before the queue filled", sequence)
		}
		if resync {
			resyncRequests++
		}
	}
	if resyncRequests != 0 {
		t.Fatalf("resync requested %d times before overflow", resyncRequests)
	}

	published, resync := c.enqueueDelta(outbound{data: []byte{1}, sequence: protocol.MaxPresenterQueueMessages + 1})
	if !published || !resync {
		t.Fatalf("overflow: published=%v resync=%v, want true/true", published, resync)
	}
	select {
	case <-c.closed:
		t.Fatal("overflow closed the connection")
	default:
	}

	// Every later delta is superseded by the pending snapshot and must not arm
	// a second resync or close the connection.
	for range 3 {
		published, resync := c.enqueueDelta(outbound{data: []byte{1}, sequence: 999})
		if !published || resync {
			t.Fatalf("pending-snapshot delta: published=%v resync=%v, want true/false", published, resync)
		}
	}
}

// A delta published after the snapshot lands extends the new baseline and must
// be preserved, not dropped, so the shell sees contiguous frames from it.
func TestSnapshotRebasesAndPreservesFollowingDeltas(t *testing.T) {
	c := activeConnection(1)
	for sequence := uint64(1); sequence <= protocol.MaxPresenterQueueMessages; sequence++ {
		c.enqueueDelta(outbound{data: []byte{1}, sequence: sequence})
	}
	if _, resync := c.enqueueDelta(outbound{data: []byte{1}, sequence: protocol.MaxPresenterQueueMessages + 1}); !resync {
		t.Fatal("overflow did not arm a resync")
	}

	baseline := uint64(1_000)
	if !c.enqueueSnapshot([]byte("snapshot"), baseline) {
		t.Fatal("enqueueSnapshot rejected a pending resync")
	}
	// A second snapshot for the same overflow is redundant and must be refused.
	if c.enqueueSnapshot([]byte("snapshot-2"), baseline+500) {
		t.Fatal("enqueueSnapshot accepted a duplicate snapshot")
	}
	for i := uint64(1); i <= 2; i++ {
		published, _ := c.enqueueDelta(outbound{data: []byte{1}, sequence: baseline + i})
		if !published {
			t.Fatalf("post-snapshot delta %d was dropped", i)
		}
	}
	// A gap in the post-snapshot tail must still close the connection rather
	// than emit a projection with a hole.
	if published, _ := c.enqueueDelta(outbound{data: []byte{1}, sequence: baseline + 9}); published {
		t.Fatal("non-contiguous post-snapshot delta was accepted")
	}
	select {
	case <-c.closed:
	default:
		t.Fatal("non-contiguous post-snapshot delta left the connection open")
	}
}

// takeResync must hand the snapshot to the writer only after the queue drains,
// then splice the buffered tail behind it and rebase lastSequence so those
// tail deltas pass the sequence check.
func TestTakeResyncDrainsQueueThenSplicesTail(t *testing.T) {
	c := activeConnection(1)
	for sequence := uint64(1); sequence <= protocol.MaxPresenterQueueMessages; sequence++ {
		c.enqueueDelta(outbound{data: []byte{1}, sequence: sequence})
	}
	c.enqueueDelta(outbound{data: []byte{1}, sequence: protocol.MaxPresenterQueueMessages + 1})
	baseline := uint64(500)
	c.enqueueSnapshot([]byte("snapshot"), baseline)
	c.enqueueDelta(outbound{data: []byte{1}, sequence: baseline + 1})
	c.enqueueDelta(outbound{data: []byte{1}, sequence: baseline + 2})

	if _, ok := c.takeResync(); ok {
		t.Fatal("takeResync fired while the queue was still draining")
	}
	// Drain the pre-snapshot frames the way writeLoop would.
	for range protocol.MaxPresenterQueueMessages {
		message := <-c.queue
		c.mu.Lock()
		c.release(message)
		c.mu.Unlock()
	}
	frame, ok := c.takeResync()
	if !ok {
		t.Fatal("takeResync did not fire once the queue drained")
	}
	if string(frame) != "snapshot" {
		t.Fatalf("takeResync frame = %q", frame)
	}
	// The two tail deltas are now resident in the queue and lastSequence is
	// rebased to the end of the tail.
	c.mu.Lock()
	queued := len(c.queue)
	last := c.lastSequence
	c.mu.Unlock()
	if queued != 2 {
		t.Fatalf("queued after resync = %d, want 2", queued)
	}
	if last != baseline+2 {
		t.Fatalf("lastSequence after resync = %d, want %d", last, baseline+2)
	}
	// The baseline is contiguous: the next delta is baseline+3.
	if published, _ := c.enqueueDelta(outbound{data: []byte{1}, sequence: baseline + 3}); !published {
		t.Fatal("connection rejected a delta continuing the rebased baseline")
	}
}
