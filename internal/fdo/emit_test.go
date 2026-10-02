package fdo

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/godbus/dbus/v5"

	"github.com/Nomadcxx/sysc-notify/internal/state"
	"github.com/Nomadcxx/sysc-notify/protocol"
)

type recordingEmitter struct {
	mu      sync.Mutex
	names   []string
	release chan struct{}
	calls   atomic.Int32
}

func (e *recordingEmitter) Emit(_ dbus.ObjectPath, name string, _ ...any) error {
	e.mu.Lock()
	e.names = append(e.names, name)
	e.mu.Unlock()
	e.calls.Add(1)
	if e.calls.Load() == 1 && e.release != nil {
		<-e.release
	}
	return nil
}

func (e *recordingEmitter) observed() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.names...)
}

func TestStalledEmitDoesNotBlockPublish(t *testing.T) {
	emitter := &recordingEmitter{release: make(chan struct{})}
	s := &Server{stop: make(chan struct{}), emitter: emitter, emitQueue: make(chan queuedSignal, emitQueueSize)}
	go s.emitLoop()
	releaseOnce := sync.OnceFunc(func() { close(emitter.release) })
	t.Cleanup(func() { releaseOnce(); close(s.stop) })

	event := state.Event{
		Delta:  &protocol.Delta{Kind: protocol.DeltaClosed, ID: 7, CloseReason: protocol.CloseDismissed},
		Action: &state.ActionEvent{ID: 7, Key: "open"},
		Reply:  &state.ReplyEvent{ID: 7, Text: "answer"},
	}
	returned := make(chan bool, 1)
	go func() { returned <- s.Publish(event) }()
	select {
	case ok := <-returned:
		if !ok {
			t.Fatal("Publish reported a drop although the queue had space")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked behind a stalled Emit")
	}

	// The first signal is parked inside the blocked Emit; releasing it must let
	// the queued rest through in order.
	releaseOnce()
	deadline := time.After(2 * time.Second)
	for emitter.calls.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("only %d signals emitted after release", emitter.calls.Load())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	want := []string{
		Interface + ".NotificationClosed",
		Interface + ".ActionInvoked",
		Interface + ".NotificationReplied",
	}
	for i, name := range want {
		if got := emitter.observed()[i]; got != name {
			t.Fatalf("signal %d = %q, want %q (order must be preserved)", i, got, name)
		}
	}
}

func TestPublishDropsOnOverflowWithoutBlocking(t *testing.T) {
	s := &Server{stop: make(chan struct{}), emitter: &recordingEmitter{}, emitQueue: make(chan queuedSignal, 1)}
	event := state.Event{
		Delta:  &protocol.Delta{Kind: protocol.DeltaClosed, ID: 1, CloseReason: protocol.CloseExpired},
		Action: &state.ActionEvent{ID: 1, Key: "open"},
	}
	if s.Publish(event) {
		t.Fatal("overflow should report a dropped signal")
	}
	if got := <-s.emitQueue; got.name != "NotificationClosed" {
		t.Fatalf("queued signal = %q, want the oldest kept", got.name)
	}
}
