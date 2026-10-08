package fdo

import (
	"reflect"
	"sync"

	"github.com/godbus/dbus/v5"
)

// CallOrder restores the sender's message order for the two calls that mutate
// state. godbus dispatches every incoming method call on its own goroutine
// (`go conn.handleCall(msg)`), so a Notify that does a bus round trip for the
// sender PID and then encodes an image can be overtaken by a CloseNotification
// the client sent after it. The update then misses its replaces_id and the
// notification is orphaned (see #54).
//
// Observe runs on godbus's single read goroutine, before dispatch, so tickets
// are handed out in true bus arrival order. Enter then admits one ticket per
// sender at a time and only around the state mutation, which keeps the slow
// preprocessing outside the ordered section and keeps different senders
// running concurrently.
type CallOrder struct {
	mu      sync.Mutex
	cond    *sync.Cond
	issued  map[string]uint64  // next ticket per sender
	turn    map[string]uint64  // ticket currently allowed to enter
	tickets map[uintptr]uint64 // per-message ticket, consumed by Enter
	closed  bool
}

const (
	notifySignature = "susssasa{sv}i"
	closeSignature  = "u"
)

// NewCallOrder returns an order that has not seen any messages yet. Install it
// with dbus.WithIncomingInterceptor and hand the same value to NewAtWithOrder.
func NewCallOrder() *CallOrder {
	order := &CallOrder{
		issued:  make(map[string]uint64),
		turn:    make(map[string]uint64),
		tickets: make(map[uintptr]uint64),
	}
	order.cond = sync.NewCond(&order.mu)
	return order
}

// Observe stamps an arrival ticket. It must be installed as an incoming
// interceptor: godbus calls it on the read goroutine, in bus order, before it
// spawns a handler goroutine for the message.
//
// Only calls that godbus will dispatch to this object are ticketed. Anything
// else (GetCapabilities, another interface, an unknown method) gets no ticket,
// and Enter passes it straight through.
func (o *CallOrder) Observe(msg *dbus.Message) {
	if o == nil || msg == nil || !o.ticketed(msg) {
		return
	}
	sender, ok := msg.Headers[dbus.FieldSender].Value().(string)
	if !ok {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	ticket := o.issued[sender]
	o.issued[sender] = ticket + 1
	o.tickets[messageID(msg)] = ticket
}

// Enter blocks until it is msg's turn for its sender and returns the release
// func for that turn. Every ticketed call must call it exactly once on every
// return path, including the paths that fail before touching state: a ticket
// that is never consumed wedges every later call from the same sender.
//
// A nil order, or a message with no ticket, returns a release that does
// nothing, so callers need no special case.
func (o *CallOrder) Enter(msg dbus.Message) func() {
	if o == nil {
		return func() {}
	}
	sender, ok := msg.Headers[dbus.FieldSender].Value().(string)
	if !ok {
		return func() {}
	}
	id := messageID(&msg)
	o.mu.Lock()
	defer o.mu.Unlock()
	ticket, ordered := o.tickets[id]
	if !ordered {
		return func() {}
	}
	delete(o.tickets, id)
	for !o.closed && o.turn[sender] != ticket {
		o.cond.Wait()
	}
	if o.closed {
		return func() {}
	}
	return func() { o.advance(sender) }
}

// advance lets the next ticket for sender run. Once every issued ticket has run
// the sender's counters are dropped, so a client that sends nothing more does
// not leak two map entries for the life of the daemon.
func (o *CallOrder) advance(sender string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.turn[sender]++
	if o.turn[sender] == o.issued[sender] {
		delete(o.turn, sender)
		delete(o.issued, sender)
	}
	o.cond.Broadcast()
}

// Close releases every waiter. Server.Close calls it so a shutdown cannot wedge
// a handler that is waiting for its turn; the object is being unexported
// immediately afterwards.
func (o *CallOrder) Close() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	o.tickets = nil
	o.cond.Broadcast()
}

// ticketed reports whether msg is one of the state-mutating calls we order.
func (o *CallOrder) ticketed(msg *dbus.Message) bool {
	// The path header decodes to a dbus.ObjectPath, and the signature header to
	// a dbus.Signature, so neither compares equal to a plain string.
	if msg.Type != dbus.TypeMethodCall ||
		msg.Headers[dbus.FieldPath].Value() != dbus.ObjectPath(ObjectPath) ||
		msg.Headers[dbus.FieldInterface].Value() != Interface {
		return false
	}
	member, _ := msg.Headers[dbus.FieldMember].Value().(string)
	signature, _ := msg.Headers[dbus.FieldSignature].Value().(dbus.Signature)
	switch member {
	case "Notify":
		return signature.String() == notifySignature
	case "CloseNotification":
		return signature.String() == closeSignature
	}
	return false
}

// messageID identifies one message across Observe and Enter. godbus copies the
// dbus.Message struct by value into the handler, but the Headers map is shared,
// so its identity is stable. The wire serial is not usable here: Message.serial
// is unexported and is never populated for an incoming message.
func messageID(msg *dbus.Message) uintptr {
	return reflect.ValueOf(msg.Headers).Pointer()
}
