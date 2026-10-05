package presenter

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/Nomadcxx/sysc-notify/internal/state"
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

// A command reply must not tear down a connection whose outbound queue is
// already full of pre-snapshot deltas. presentation.renew is the keepalive
// that holds the expiry lease, so it has to be answered while resync can
// still rebase. The queue is left undrained until the reply is accepted, so
// the send hits the full-queue path rather than racing the writer.
func TestCommandReplyWhileDeltaQueueIsFull(t *testing.T) {
	for _, armed := range []bool{false, true} {
		name := "at capacity"
		if armed {
			name = "resync armed"
		}
		t.Run(name, func(t *testing.T) {
			testCommandReplyWhileDeltaQueueIsFull(t, armed)
		})
	}
}

func testCommandReplyWhileDeltaQueueIsFull(t *testing.T, armResync bool) {
	t.Helper()
	owner := state.Start(newPresenterClock(), funcSink(func(state.Event) bool { return true }))
	t.Cleanup(func() { _ = owner.Close() })
	id := addCandidate(t, owner, "visible", time.Minute)

	client, server := unixPair(t)
	defer client.Close()
	conn := newConnection(server, 1)
	if !conn.activate(0) {
		t.Fatal("activate")
	}

	for sequence := uint64(1); sequence <= protocol.MaxPresenterQueueMessages; sequence++ {
		published, resync := conn.enqueueDelta(outbound{data: []byte{1}, sequence: sequence})
		if !published || resync {
			t.Fatalf("delta %d: published=%v resync=%v", sequence, published, resync)
		}
	}
	if armResync {
		published, resync := conn.enqueueDelta(outbound{data: []byte{1}, sequence: protocol.MaxPresenterQueueMessages + 1})
		if !published || !resync {
			t.Fatalf("overflow: published=%v resync=%v", published, resync)
		}
	}

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		conn.readLoop(owner)
	}()
	writerStarted := false
	defer func() {
		conn.fail()
		<-readDone
		if writerStarted {
			<-conn.writerDone
		}
	}()

	command := protocol.Command{
		Kind:          protocol.CommandPresentationRenew,
		Presentations: []protocol.Presentation{{ID: id, State: protocol.PresentationVisible}},
	}
	if err := writeEnvelope(client, protocol.KindCommand, 1, 0, command); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		conn.mu.Lock()
		held := len(conn.replyHold) > 0
		conn.mu.Unlock()
		if held {
			break
		}
		select {
		case <-conn.closed:
			t.Fatal("renew closed the connection while the delta queue was full")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("reply was not accepted onto a full delta queue")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-conn.closed:
		t.Fatal("parking the reply closed the connection")
	default:
	}

	if armResync && !conn.enqueueSnapshot([]byte("snapshot"), 1_000) {
		t.Fatal("enqueueSnapshot rejected a resync that a reply should have left armed")
	}
	writerStarted = true
	go conn.writeLoop()

	reply, sawSnapshot := readReplyPastQueue(t, client, 1, armResync)
	if !reply.OK || reply.Error != nil {
		t.Fatalf("reply = %#v", reply)
	}
	if armResync && !sawSnapshot {
		t.Fatal("resync snapshot was not written after the reply")
	}
	select {
	case <-conn.closed:
		t.Fatal("delivering the reply closed the connection")
	default:
	}
}

func readReplyPastQueue(t *testing.T, conn net.Conn, requestID uint64, wantSnapshot bool) (protocol.Reply, bool) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var reply protocol.Reply
	sawReply := false
	sawSnapshot := false
	for !sawReply || (wantSnapshot && !sawSnapshot) {
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		switch {
		case bytes.Equal(frame, []byte{1}):
			continue
		case bytes.Equal(frame, []byte("snapshot")):
			sawSnapshot = true
		default:
			var envelope protocol.Envelope
			if err := protocol.DecodeStrict(frame, &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Kind != protocol.KindReply {
				continue
			}
			if envelope.RequestID != requestID {
				t.Fatalf("reply request ID = %d, want %d", envelope.RequestID, requestID)
			}
			decodePayload(t, envelope, &reply)
			if err := reply.Validate(); err != nil {
				t.Fatal(err)
			}
			sawReply = true
		}
	}
	return reply, sawSnapshot
}
