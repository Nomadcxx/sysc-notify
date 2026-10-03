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

	closed     chan struct{}
	writerDone chan struct{}
	failOnce   sync.Once
}

func newConnection(socket *net.UnixConn, generation uint64) *connection {
	ctx, cancel := context.WithCancel(context.Background())
	return &connection{
		socket: socket, generation: generation, ctx: ctx, cancel: cancel,
		queue:  make(chan outbound, protocol.MaxPresenterQueueMessages),
		closed: make(chan struct{}), writerDone: make(chan struct{}),
	}
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

func (c *connection) enqueueDelta(message outbound) bool {
	c.mu.Lock()
	if c.failed || !c.active || protocol.ValidateNextSequence(c.lastSequence, message.sequence) != nil || !c.reserve(message) {
		c.mu.Unlock()
		c.fail()
		return false
	}
	c.lastSequence = message.sequence
	select {
	case c.queue <- message:
		c.mu.Unlock()
		return true
	default:
		c.release(message)
		c.mu.Unlock()
		c.fail()
		return false
	}
}

func (c *connection) enqueue(message outbound) bool {
	c.mu.Lock()
	if c.failed || !c.active || !c.reserve(message) {
		c.mu.Unlock()
		c.fail()
		return false
	}
	select {
	case c.queue <- message:
		c.mu.Unlock()
		return true
	default:
		c.release(message)
		c.mu.Unlock()
		c.fail()
		return false
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

func (c *connection) release(message outbound) {
	c.queuedMessages--
	c.queuedBytes -= len(message.data)
}

func (c *connection) fail() {
	c.failOnce.Do(func() {
		c.mu.Lock()
		c.failed = true
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
		select {
		case <-c.closed:
			return
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
