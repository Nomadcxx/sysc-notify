package presenter

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"

	"github.com/Nomadcxx/sysc-notify/internal/state"
	"github.com/Nomadcxx/sysc-notify/protocol"
)

type outbound struct {
	data     []byte
	sequence uint64
}

type connection struct {
	socket     *net.UnixConn
	generation uint64
	producer   bool
	ctx        context.Context
	cancel     context.CancelFunc

	mu             sync.Mutex
	queue          chan outbound
	pending        []outbound
	queuedMessages int
	queuedBytes    int
	lastSequence   uint64
	lastRequestID  uint64
	active         bool
	failed         bool
	// resyncNeeded is set when the bounded queue overflowed. While set, the
	// writer swaps in a fresh snapshot once the queue has drained, rebasing the
	// projection instead of tearing the connection down. Deltas published after
	// that snapshot arrive before it is written and are held in resyncTail so
	// they stay contiguous with the new baseline.
	resyncNeeded    bool
	resyncScheduled bool
	resyncFrame     []byte
	resyncSeq       uint64
	resyncTail      []outbound
	resyncTailBytes int
	// replyHold is one command reply parked because the outbound queue is
	// already at MaxPresenterQueueMessages or MaxPresenterDecodedBytes. Those
	// frames are the pre-snapshot backlog: a delta arms resync instead of
	// failing, and this reply must not fail the connection either. The writer
	// emits it ahead of that backlog. A second reply waits on replyWait, so
	// the hold stays a single frame and the queue caps are unchanged.
	replyHold []byte
	replyWait sync.Cond

	closed     chan struct{}
	writerDone chan struct{}
	wake       chan struct{}
	failOnce   sync.Once
}

func newConnection(socket *net.UnixConn, generation uint64) *connection {
	ctx, cancel := context.WithCancel(context.Background())
	c := &connection{
		socket: socket, generation: generation, ctx: ctx, cancel: cancel,
		queue:  make(chan outbound, protocol.MaxPresenterQueueMessages),
		closed: make(chan struct{}), writerDone: make(chan struct{}), wake: make(chan struct{}, 1),
	}
	c.replyWait.L = &c.mu
	return c
}

func (c *connection) prepare(message outbound) bool {
	c.mu.Lock()
	if c.failed || !c.reserve(message) {
		c.mu.Unlock()
		c.fail()
		return false
	}
	c.pending = append(c.pending, message)
	c.mu.Unlock()
	return true
}

func (c *connection) activate(sequence uint64) bool {
	c.mu.Lock()
	if c.failed {
		c.mu.Unlock()
		return false
	}
	c.lastSequence = sequence
	for _, message := range c.pending {
		if message.sequence <= sequence {
			c.release(message)
			continue
		}
		if protocol.ValidateNextSequence(c.lastSequence, message.sequence) != nil {
			c.mu.Unlock()
			c.fail()
			return false
		}
		c.lastSequence = message.sequence
		c.queue <- message
	}
	c.pending = nil
	c.active = true
	c.mu.Unlock()
	return true
}

// enqueueDelta queues one delta. It reports whether the connection is still
// usable and, when it is, whether the caller must arrange a resync because the
// bounded queue overflowed. A resync is requested instead of failing so a slow
// reader loses intermediate frames but keeps its generation.
func (c *connection) enqueueDelta(message outbound) (published bool, resync bool) {
	c.mu.Lock()
	if c.failed || !c.active {
		c.mu.Unlock()
		return false, false
	}
	if c.resyncNeeded {
		// Before the snapshot lands, deltas are superseded by it and dropped:
		// the owner builds the snapshot only after every one of them, so the
		// snapshot's sequence covers them all.
		if c.resyncFrame == nil {
			c.mu.Unlock()
			return true, false
		}
		// After it lands, later deltas extend the new baseline and must be kept
		// so the projection stays contiguous from the snapshot. The queue is
		// still full of pre-snapshot frames, so the tail is bounded separately.
		expected := c.resyncSeq + uint64(len(c.resyncTail)) + 1
		if message.sequence != expected || !c.reserveTail(message) {
			c.mu.Unlock()
			c.fail()
			return false, false
		}
		c.resyncTail = append(c.resyncTail, message)
		c.mu.Unlock()
		return true, false
	}
	if protocol.ValidateNextSequence(c.lastSequence, message.sequence) != nil {
		c.mu.Unlock()
		c.fail()
		return false, false
	}
	if !c.reserve(message) {
		resync = c.beginResyncLocked()
		c.mu.Unlock()
		return true, resync
	}
	c.lastSequence = message.sequence
	select {
	case c.queue <- message:
		c.mu.Unlock()
		return true, false
	default:
		c.release(message)
		resync = c.beginResyncLocked()
		c.mu.Unlock()
		return true, resync
	}
}

// beginResyncLocked arms a snapshot swap and reports whether this call is the
// one that must ask the owner for it. Callers hold c.mu.
func (c *connection) beginResyncLocked() bool {
	c.resyncNeeded = true
	if c.resyncScheduled {
		return false
	}
	c.resyncScheduled = true
	return true
}

// enqueueSnapshot stores a freshly built snapshot and wakes the writer. The
// writer drains any queued deltas first, so the snapshot's sequence is a clean
// baseline for the frames that follow it.
func (c *connection) enqueueSnapshot(frame []byte, sequence uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed || !c.active || !c.resyncNeeded || c.resyncFrame != nil {
		return false
	}
	c.resyncFrame = frame
	c.resyncSeq = sequence
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return true
}

// enqueue queues a command reply. The delta queue's message and byte caps
// still apply to anything placed on c.queue. A reply that arrives while those
// caps are held by pre-snapshot deltas is parked in replyHold instead of
// failing the connection; the writer emits that one frame before the backlog.
func (c *connection) enqueue(message outbound) bool {
	c.mu.Lock()
	for {
		if c.failed || !c.active {
			c.mu.Unlock()
			c.fail()
			return false
		}
		// Keep reply order: a parked reply has to leave before the next one
		// is admitted, whether onto the queue or into the hold.
		if c.replyHold != nil {
			c.replyWait.Wait()
			continue
		}
		if c.reserve(message) {
			select {
			case c.queue <- message:
				c.mu.Unlock()
				return true
			default:
				c.release(message)
			}
		}
		c.replyHold = message.data
		c.mu.Unlock()
		select {
		case c.wake <- struct{}{}:
		default:
		}
		return true
	}
}

func (c *connection) reserve(message outbound) bool {
	if c.queuedMessages >= protocol.MaxPresenterQueueMessages ||
		c.queuedBytes > protocol.MaxPresenterDecodedBytes-len(message.data) {
		return false
	}
	c.queuedMessages++
	c.queuedBytes += len(message.data)
	return true
}

// reserveTail bounds the post-snapshot buffer independently of the main queue,
// which is still draining its pre-snapshot frames when the tail fills.
func (c *connection) reserveTail(message outbound) bool {
	if len(c.resyncTail) >= protocol.MaxPresenterQueueMessages ||
		c.resyncTailBytes > protocol.MaxPresenterDecodedBytes-len(message.data) {
		return false
	}
	c.resyncTailBytes += len(message.data)
	return true
}

func (c *connection) release(message outbound) {
	c.queuedMessages--
	c.queuedBytes -= len(message.data)
}

func (c *connection) fail() {
	c.failOnce.Do(func() {
		c.mu.Lock()
		c.failed = true
		c.replyWait.Broadcast()
		c.mu.Unlock()
		close(c.closed)
		if c.socket != nil {
			_ = c.socket.Close()
		}
		// Unblock owner.Do calls not yet received by the owner; commands
		// arriving after PresenterLost are rejected by the generation gate.
		c.cancel()
	})
}

func (c *connection) writeLoop() {
	defer close(c.writerDone)
	for {
		// A reply parked against a full pre-snapshot queue is not part of the
		// delta backlog. Emit it before those frames so the command result is
		// not stuck behind a resync.
		if frame, ok := c.takeReply(); ok {
			if err := protocol.WriteFrame(c.socket, frame); err != nil {
				c.fail()
				return
			}
			continue
		}
		// A scheduled snapshot is written only after every queued delta has
		// drained, so the baseline never trails a frame already on the wire.
		if frame, ok := c.takeResync(); ok {
			if err := protocol.WriteFrame(c.socket, frame); err != nil {
				c.fail()
				return
			}
			continue
		}
		select {
		case <-c.closed:
			return
		case <-c.wake:
			continue
		case message := <-c.queue:
			if err := protocol.WriteFrame(c.socket, message.data); err != nil {
				c.fail()
				return
			}
			c.mu.Lock()
			c.release(message)
			c.mu.Unlock()
		}
	}
}

// takeReply removes the parked command reply, if any. Callers do not hold c.mu.
func (c *connection) takeReply() ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.replyHold == nil {
		return nil, false
	}
	frame := c.replyHold
	c.replyHold = nil
	c.replyWait.Signal()
	return frame, true
}

// takeResync consumes a pending snapshot once the queue is empty. It writes the
// snapshot ahead of the buffered post-snapshot deltas and rebases lastSequence
// so those deltas continue contiguously from the new baseline.
func (c *connection) takeResync() ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.resyncFrame == nil || len(c.queue) != 0 {
		return nil, false
	}
	frame := c.resyncFrame
	c.resyncFrame = nil
	// The tail was reserved against the same bound as the queue, and the queue
	// is empty, so moving it over cannot block.
	for _, message := range c.resyncTail {
		c.queue <- message
		c.queuedMessages++
		c.queuedBytes += len(message.data)
	}
	c.lastSequence = c.resyncSeq + uint64(len(c.resyncTail))
	c.resyncTail = nil
	c.resyncTailBytes = 0
	c.resyncSeq = 0
	c.resyncNeeded = false
	c.resyncScheduled = false
	return frame, true
}

func (c *connection) readLoop(owner *state.Owner) {
	for {
		frame, err := protocol.ReadFrame(c.socket)
		if err != nil {
			return
		}
		var envelope protocol.Envelope
		if protocol.DecodeStrict(frame, &envelope) != nil || envelope.Validate() != nil || envelope.Kind != protocol.KindCommand {
			return
		}
		if envelope.RequestID <= c.lastRequestID {
			return
		}
		var command protocol.Command
		if protocol.DecodeStrict(envelope.Payload, &command) != nil || command.Validate() != nil {
			return
		}
		c.lastRequestID = envelope.RequestID
		reply := executeCommandForConnection(c.ctx, owner, c.generation, c.producer, command)
		payload, err := marshalEnvelope(protocol.KindReply, envelope.RequestID, 0, reply)
		if err != nil || !c.enqueue(outbound{data: payload}) {
			return
		}
	}
}

func executeCommand(owner *state.Owner, generation uint64, command protocol.Command) protocol.Reply {
	return executeCommandForConnection(context.Background(), owner, generation, false, command)
}

func executeCommandForConnection(ctx context.Context, owner *state.Owner, generation uint64, producer bool, command protocol.Command) protocol.Reply {
	if (command.Kind == protocol.CommandProducerPublish || command.Kind == protocol.CommandProducerClose) && !producer {
		return protocol.Reply{Error: &protocol.ProtocolError{Code: protocol.ErrorUnavailable, Message: "producer capability was not negotiated"}}
	}
	stateCommand := state.Command{
		ID: command.ID, ActionKey: command.ActionKey, ReplyText: command.Text,
		IDs: append([]uint32(nil), command.IDs...), Generation: generation,
	}
	switch command.Kind {
	case protocol.CommandPresentationRenew:
		stateCommand.Kind = state.PresentationRenew
		stateCommand.Presentations = append([]protocol.Presentation(nil), command.Presentations...)
	case protocol.CommandAction:
		stateCommand.Kind = state.InvokeAction
	case protocol.CommandDismiss:
		stateCommand.Kind = state.Dismiss
	case protocol.CommandReply:
		stateCommand.Kind = state.SubmitReply
	case protocol.CommandHistoryClear:
		stateCommand.Kind = state.HistoryClear
	case protocol.CommandHistoryRemove:
		stateCommand.Kind = state.HistoryRemove
	case protocol.CommandHistoryMarkSeen:
		stateCommand.Kind = state.HistoryMarkSeen
	case protocol.CommandDismissAll:
		stateCommand.Kind = state.DismissAll
	case protocol.CommandProducerPublish:
		stateCommand.Kind = state.ProducerPublish
		stateCommand.Producer = command.Producer
	case protocol.CommandProducerClose:
		stateCommand.Kind = state.ProducerClose
		stateCommand.Producer = command.Producer
	}
	result, err := owner.Do(ctx, stateCommand)
	if err == nil {
		return protocol.Reply{OK: true, ID: result.ID, Replaced: result.Replaced, Lifetimes: result.Lifetimes}
	}
	code := protocol.ErrorInvalid
	if errors.Is(err, state.ErrNotFound) {
		code = protocol.ErrorNotFound
	} else if errors.Is(err, state.ErrStale) {
		code = protocol.ErrorStale
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		code = protocol.ErrorUnavailable
	}
	return protocol.Reply{Error: &protocol.ProtocolError{Code: code, Message: err.Error()}}
}

func marshalEnvelope(kind string, requestID, sequence uint64, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(protocol.Envelope{Kind: kind, RequestID: requestID, Sequence: sequence, Payload: raw})
}
