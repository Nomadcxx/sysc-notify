package state

import (
	"context"
	"errors"
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

func TestSnapshotAndRenewExposeAuthoritativeLifetime(t *testing.T) {
	clock, owner := startClockOwner(t)
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("countdown", 10*time.Second)}).ID

	initial := lifetimeFor(t, snapshot(t, owner).Lifetimes, id)
	if initial.DurationMS != 10_000 || initial.RemainingMS != 10_000 || !initial.Running {
		t.Fatalf("initial lifetime = %#v", initial)
	}

	clock.Advance(3 * time.Second)
	paused := do(t, owner, Command{
		Kind: PresentationRenew, Generation: 1,
		Presentations: []protocol.Presentation{{ID: id, State: protocol.PresentationHovered}},
	})
	got := lifetimeFor(t, paused.Lifetimes, id)
	if got.DurationMS != 10_000 || got.RemainingMS != 7_000 || got.Running {
		t.Fatalf("paused lifetime = %#v", got)
	}
	if reconnect := lifetimeFor(t, snapshot(t, owner).Lifetimes, id); reconnect != got {
		t.Fatalf("reconnect lifetime = %#v, want %#v", reconnect, got)
	}

	resumed := do(t, owner, Command{
		Kind: PresentationRenew, Generation: 1,
		Presentations: []protocol.Presentation{{ID: id, State: protocol.PresentationVisible}},
	})
	if got := lifetimeFor(t, resumed.Lifetimes, id); got.RemainingMS != 7_000 || !got.Running {
		t.Fatalf("resumed lifetime = %#v", got)
	}
}

func lifetimeFor(t *testing.T, lifetimes []protocol.Lifetime, id uint32) protocol.Lifetime {
	t.Helper()
	for _, lifetime := range lifetimes {
		if lifetime.ID == id {
			return lifetime
		}
	}
	t.Fatalf("lifetime %d missing from %#v", id, lifetimes)
	return protocol.Lifetime{}
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

func TestStaleRenewDoesNotStealLiveLease(t *testing.T) {
	clock, owner := startClockOwner(t)
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("live", 10*time.Second)}).ID
	present(t, owner, 2, id, protocol.PresentationQueued)

	_, err := owner.Do(context.Background(), Command{
		Kind: PresentationRenew, Generation: 1,
		Presentations: []protocol.Presentation{{ID: id, State: protocol.PresentationVisible}},
	})
	if !errors.Is(err, ErrStale) {
		t.Fatalf("older renew error = %v, want ErrStale", err)
	}
	clock.Advance(10 * time.Second)
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("stale renew replaced the live presenter lease")
	}
}

func TestRetiredGenerationCannotReinstallLease(t *testing.T) {
	clock, owner := startClockOwner(t)
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("retired", 10*time.Second)}).ID
	present(t, owner, 1, id, protocol.PresentationQueued)
	do(t, owner, Command{Kind: PresenterLost, Generation: 1})

	_, err := owner.Do(context.Background(), Command{
		Kind: PresentationRenew, Generation: 1,
		Presentations: []protocol.Presentation{{ID: id, State: protocol.PresentationQueued}},
	})
	if !errors.Is(err, ErrStale) {
		t.Fatalf("renew after disconnect error = %v, want ErrStale", err)
	}
	clock.Advance(10 * time.Second)
	waitForMissing(t, owner, id)
}

func TestSameGenerationRenewsAfterLeaseExpiry(t *testing.T) {
	clock, owner := startClockOwner(t)
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("held", 10*time.Second)}).ID
	present(t, owner, 1, id, protocol.PresentationQueued)
	clock.Advance(PresentationLease)
	present(t, owner, 1, id, protocol.PresentationQueued)
	clock.Advance(10 * time.Second)
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("lease expiry retired the connected presenter")
	}
}

func TestOlderPresenterLostDoesNotDropNewerLease(t *testing.T) {
	clock, owner := startClockOwner(t)
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("newer", 10*time.Second)}).ID
	present(t, owner, 1, id, protocol.PresentationQueued)
	present(t, owner, 2, id, protocol.PresentationQueued)
	do(t, owner, Command{Kind: PresenterLost, Generation: 1})
	clock.Advance(10 * time.Second)
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("presenter lost for an older generation cleared the live lease")
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

func TestCriticalDefaultTimeoutStaysUntilDismissed(t *testing.T) {
	clock, owner := startClockOwner(t)
	critical := candidate("disk", -time.Millisecond)
	critical.Urgency = protocol.UrgencyCritical
	id := do(t, owner, Command{Kind: Add, Candidate: critical}).ID

	clock.Advance(DefaultTimeout)
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("critical notification with the server default timeout expired at 5s")
	}
	clock.Advance(24 * time.Hour)
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("critical notification with the server default timeout expired")
	}

	normal := candidate("note", -time.Millisecond)
	normalID := do(t, owner, Command{Kind: Add, Candidate: normal}).ID
	clock.Advance(DefaultTimeout - time.Millisecond)
	if !hasID(snapshot(t, owner), normalID) {
		t.Fatal("non-critical default timeout expired early")
	}
	clock.Advance(time.Millisecond)
	waitForMissing(t, owner, normalID)
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("critical notification expired alongside the normal one")
	}
}

func TestCriticalExplicitTimeoutStillExpires(t *testing.T) {
	clock, owner := startClockOwner(t)
	critical := candidate("disk", 2*time.Second)
	critical.Urgency = protocol.UrgencyCritical
	id := do(t, owner, Command{Kind: Add, Candidate: critical}).ID
	clock.Advance(2*time.Second - time.Millisecond)
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("explicit critical timeout expired early")
	}
	clock.Advance(time.Millisecond)
	waitForMissing(t, owner, id)
}

func TestDefaultTimeoutReplacementFollowsNewUrgency(t *testing.T) {
	clock, owner := startClockOwner(t)
	critical := candidate("disk", -time.Millisecond)
	critical.Urgency = protocol.UrgencyCritical
	id := do(t, owner, Command{Kind: Add, Candidate: critical}).ID

	normal := candidate("note", -time.Millisecond)
	normal.ReplacesID = id
	do(t, owner, Command{Kind: Add, Candidate: normal})
	clock.Advance(DefaultTimeout)
	waitForMissing(t, owner, id)

	id = do(t, owner, Command{Kind: Add, Candidate: candidate("note", -time.Millisecond)}).ID
	critical = candidate("disk", -time.Millisecond)
	critical.Urgency = protocol.UrgencyCritical
	critical.ReplacesID = id
	do(t, owner, Command{Kind: Add, Candidate: critical})
	clock.Advance(DefaultTimeout * 2)
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("critical replacement of a default-timeout notification expired")
	}
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

func TestSuppressedAfterDeadlineExpiresInsteadOfRestarting(t *testing.T) {
	clock, owner := startClockOwner(t)
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("late", 10*time.Second)}).ID
	present(t, owner, 1, id, protocol.PresentationVisible)
	// The renew wins the owner's select against the due timer.
	clock.Jump(10 * time.Second)
	present(t, owner, 1, id, protocol.PresentationHovered)
	present(t, owner, 1, id, protocol.PresentationSuppressed)
	clock.Advance(0)
	waitForMissing(t, owner, id)
}

func TestRenewSkipsNotificationsThatAlreadyClosed(t *testing.T) {
	clock, owner := startClockOwner(t)
	kept := do(t, owner, Command{Kind: Add, Candidate: candidate("kept", 10*time.Second)}).ID
	gone := do(t, owner, Command{Kind: Add, Candidate: candidate("gone", 10*time.Second)}).ID
	do(t, owner, Command{Kind: Dismiss, ID: gone})
	result := do(t, owner, Command{
		Kind: PresentationRenew, Generation: 1,
		Presentations: []protocol.Presentation{
			{ID: kept, State: protocol.PresentationHovered},
			{ID: gone, State: protocol.PresentationVisible},
		},
	})
	if lifetime := lifetimeFor(t, result.Lifetimes, kept); lifetime.Running {
		t.Fatalf("hovered lifetime = %#v, want paused", lifetime)
	}
	holdPresentation(t, clock, owner, 1, kept, protocol.PresentationHovered, time.Minute)
	if !hasID(snapshot(t, owner), kept) {
		t.Fatal("hover was not applied or lease starved")
	}
}

// Jump moves the clock without firing timers, as when a request wins the
// owner's select over a timer that is already due.
func (c *manualClock) Jump(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}
