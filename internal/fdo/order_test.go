package fdo

import (
	"testing"
	"time"

	"github.com/godbus/dbus/v5"
)

// orderMessage builds a message shaped like one arriving for our interface.
func orderMessage(sender, member, signature string) *dbus.Message {
	// Real messages carry a dbus.Signature, not a plain string.
	sig := dbus.ParseSignatureMust(signature)
	return &dbus.Message{
		Type: dbus.TypeMethodCall,
		Headers: map[dbus.HeaderField]dbus.Variant{
			dbus.FieldPath:      dbus.MakeVariant(dbus.ObjectPath(ObjectPath)),
			dbus.FieldInterface: dbus.MakeVariant(Interface),
			dbus.FieldMember:    dbus.MakeVariant(member),
			dbus.FieldSignature: dbus.MakeVariant(sig),
			dbus.FieldSender:    dbus.MakeVariant(sender),
		},
	}
}

func TestCallOrderBlocksLaterTicketForSameSender(t *testing.T) {
	order := NewCallOrder()
	first := orderMessage(":1.1", "Notify", notifySignature)
	second := orderMessage(":1.1", "CloseNotification", "u")
	order.Observe(first)
	order.Observe(second)

	// The later call's handler runs first, as godbus does; it must wait.
	entered := make(chan func(), 1)
	go func() { entered <- order.Enter(*second) }()
	select {
	case <-entered:
		t.Fatal("second ticket entered before the first released it")
	case <-time.After(50 * time.Millisecond):
	}

	firstEntered := make(chan func(), 1)
	go func() { firstEntered <- order.Enter(*first) }()
	select {
	case release := <-firstEntered:
		release()
	case <-time.After(time.Second):
		t.Fatal("first ticket did not enter on its turn")
	}

	select {
	case release := <-entered:
		release()
	case <-time.After(time.Second):
		t.Fatal("second ticket stayed blocked after the first released")
	}
}

func TestCallOrderDoesNotSerializeDifferentSenders(t *testing.T) {
	order := NewCallOrder()
	busy := orderMessage(":1.1", "Notify", notifySignature)
	other := orderMessage(":1.2", "CloseNotification", "u")
	order.Observe(busy)
	order.Observe(other)

	releaseBusy := order.Enter(*busy)
	if releaseBusy == nil {
		t.Fatal("Enter returned no release func")
	}
	// The busy sender still holds its turn, so the other sender must be free.
	done := make(chan struct{})
	go func() {
		order.Enter(*other)()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a second sender was blocked behind the first sender's turn")
	}
	releaseBusy()
}

func TestCallOrderCloseUnblocksWaiters(t *testing.T) {
	order := NewCallOrder()
	first := orderMessage(":1.1", "Notify", notifySignature)
	second := orderMessage(":1.1", "CloseNotification", "u")
	order.Observe(first)
	order.Observe(second)

	done := make(chan struct{})
	go func() {
		order.Enter(*second)()
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	order.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not release a waiter")
	}
	// Close is idempotent and a closed order admits nothing further.
	order.Close()
	order.Enter(*first)()
}

func TestCallOrderPassesUnticketedMessagesThrough(t *testing.T) {
	order := NewCallOrder()
	order.Observe(orderMessage(":1.1", "GetCapabilities", ""))
	// Never observed: no ticket, so Enter must not block.
	done := make(chan struct{})
	go func() {
		order.Enter(*orderMessage(":1.1", "Notify", notifySignature))()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("an untracked message blocked in Enter")
	}

	// A message from another interface is not ticketed either.
	foreign := &dbus.Message{Type: dbus.TypeMethodCall, Headers: map[dbus.HeaderField]dbus.Variant{
		dbus.FieldPath:      dbus.MakeVariant(dbus.ObjectPath("/org/other")),
		dbus.FieldInterface: dbus.MakeVariant("org.other"),
		dbus.FieldMember:    dbus.MakeVariant("Notify"),
		dbus.FieldSignature: dbus.MakeVariant(notifySignature),
		dbus.FieldSender:    dbus.MakeVariant(":1.3"),
	}}
	order.Observe(foreign)
	if len(order.tickets) != 0 {
		t.Fatal("a call to another interface was ticketed")
	}
	order.Enter(*foreign)()
}

func TestCallOrderDropsIdleSenderEntries(t *testing.T) {
	order := NewCallOrder()
	msg := orderMessage(":1.7", "CloseNotification", "u")
	order.Observe(msg)
	order.Enter(*msg)()
	order.mu.Lock()
	defer order.mu.Unlock()
	if len(order.issued) != 0 || len(order.turn) != 0 || len(order.tickets) != 0 {
		t.Fatalf("idle sender kept entries: issued=%v turn=%v tickets=%v",
			order.issued, order.turn, order.tickets)
	}
}

func TestNilCallOrderIsInert(t *testing.T) {
	var order *CallOrder
	order.Observe(orderMessage(":1.1", "Notify", notifySignature))
	order.Close()
	order.Enter(*orderMessage(":1.1", "Notify", notifySignature))()
}
