package state

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

func TestQueuedBeforeFirstDisplayStartsFullTimeoutWhenVisible(t *testing.T) {
	clock, owner := startClockOwner(t)
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("queued", 10*time.Second)}).ID
	present(t, owner, 1, id, protocol.PresentationQueued)
	holdPresentation(t, clock, owner, 1, id, protocol.PresentationQueued, time.Minute)
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("queued notification expired before display")
	}
	present(t, owner, 1, id, protocol.PresentationVisible)
	clock.Advance(9 * time.Second)
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("visible notification expired early")
	}
	clock.Advance(time.Second)
	waitForMissing(t, owner, id)
}

func TestHoverPausesAndVisibleResumesRemainingTimeout(t *testing.T) {
	clock, owner := startClockOwner(t)
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("hover", 10*time.Second)}).ID
	present(t, owner, 1, id, protocol.PresentationVisible)
	clock.Advance(3 * time.Second)
	present(t, owner, 1, id, protocol.PresentationHovered)
	holdPresentation(t, clock, owner, 1, id, protocol.PresentationHovered, time.Minute)
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("hovered notification expired")
	}
	present(t, owner, 1, id, protocol.PresentationVisible)
	clock.Advance(6 * time.Second)
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("notification did not preserve remaining timeout")
	}
	clock.Advance(time.Second)
	waitForMissing(t, owner, id)
}

func TestSuppressedQueuedNotificationCannotSurviveForever(t *testing.T) {
	clock, owner := startClockOwner(t)
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("suppressed", 10*time.Second)}).ID
	present(t, owner, 1, id, protocol.PresentationQueued)
	present(t, owner, 1, id, protocol.PresentationSuppressed)
	clock.Advance(10 * time.Second)
	waitForMissing(t, owner, id)
}

func TestPresentationLeaseExpiryAndDisconnectClearHolds(t *testing.T) {
	for name, release := range map[string]func(*testing.T, *manualClock, *Owner, uint64){
		"lease timeout": func(t *testing.T, clock *manualClock, _ *Owner, _ uint64) { clock.Advance(PresentationLease) },
		"disconnect": func(t *testing.T, _ *manualClock, owner *Owner, generation uint64) {
			do(t, owner, Command{Kind: PresenterLost, Generation: generation})
		},
	} {
		t.Run(name, func(t *testing.T) {
			clock, owner := startClockOwner(t)
			id := do(t, owner, Command{Kind: Add, Candidate: candidate(name, 10*time.Second)}).ID
			present(t, owner, 7, id, protocol.PresentationQueued)
			release(t, clock, owner, 7)
			clock.Advance(10 * time.Second)
			waitForMissing(t, owner, id)
		})
	}
}

func TestPresenterReplacementClearsOldHolds(t *testing.T) {
	clock, owner := startClockOwner(t)
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("generation", 10*time.Second)}).ID
	present(t, owner, 1, id, protocol.PresentationQueued)
	present(t, owner, 2, id, protocol.PresentationSuppressed)
	clock.Advance(10 * time.Second)
	waitForMissing(t, owner, id)
}

func TestReplacementTimeoutUsesCurrentPresentationState(t *testing.T) {
	clock, owner := startClockOwner(t)
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("old", 30*time.Second)}).ID
	present(t, owner, 1, id, protocol.PresentationHovered)
	replacement := candidate("new", 5*time.Second)
	replacement.ReplacesID = id
	do(t, owner, Command{Kind: Add, Candidate: replacement})
	holdPresentation(t, clock, owner, 1, id, protocol.PresentationHovered, time.Minute)
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("hovered replacement expired")
	}
	present(t, owner, 1, id, protocol.PresentationVisible)
	clock.Advance(5 * time.Second)
	waitForMissing(t, owner, id)
}

func TestExpiryEmitsExpiredCloseReason(t *testing.T) {
	clock := newManualClock()
	sink := newEventSink()
	owner := Start(clock, sink)
	t.Cleanup(func() { _ = owner.Close() })
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("expiry", time.Second)}).ID
	clock.Advance(time.Second)
	waitForMissing(t, owner, id)
	events := sink.Events()
	last := events[len(events)-1]
	if last.Delta == nil || last.Delta.CloseReason != protocol.CloseExpired {
		t.Fatalf("last event = %#v", last)
	}
}

func present(t *testing.T, owner *Owner, generation uint64, id uint32, state protocol.PresentationState) {
	t.Helper()
	do(t, owner, Command{
		Kind: PresentationRenew, Generation: generation,
		Presentations: []protocol.Presentation{{ID: id, State: state}},
	})
}

func holdPresentation(t *testing.T, clock *manualClock, owner *Owner, generation uint64, id uint32, state protocol.PresentationState, duration time.Duration) {
	t.Helper()
	for duration > 0 {
		step := min(2*time.Second, duration)
		clock.Advance(step)
		present(t, owner, generation, id, state)
		duration -= step
	}
}

func startClockOwner(t *testing.T) (*manualClock, *Owner) {
	t.Helper()
	clock := newManualClock()
	owner := Start(clock, newEventSink())
	t.Cleanup(func() { _ = owner.Close() })
	return clock, owner
}

func waitForMissing(t *testing.T, owner *Owner, id uint32) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !hasID(snapshot(t, owner), id) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("notification %d remains active", id)
}

type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*manualTimer]struct{}
}

func newManualClock() *manualClock {
	return &manualClock{now: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), timers: make(map[*manualTimer]struct{})}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) NewTimer(duration time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &manualTimer{clock: c, at: c.now.Add(duration), ch: make(chan time.Time, 1)}
	c.timers[timer] = struct{}{}
	return timer
}

func (c *manualClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	now := c.now
	for timer := range c.timers {
		if !timer.stopped && !timer.at.After(now) {
			timer.stopped = true
			timer.ch <- now
		}
	}
	c.mu.Unlock()
}

type manualTimer struct {
	clock   *manualClock
	at      time.Time
	ch      chan time.Time
	stopped bool
}

func (t *manualTimer) C() <-chan time.Time { return t.ch }

func (t *manualTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := !t.stopped
	t.stopped = true
	delete(t.clock.timers, t)
	return wasActive
}

func TestOwnerDoHonorsCancelledContext(t *testing.T) {
	owner, _ := startTestOwner(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := owner.Do(ctx, Command{Kind: Add, Candidate: candidate("cancelled", time.Second)}); err == nil {
		t.Fatal("Do() ignored cancelled context")
	}
}
