package state

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Nomadcxx/sysc-notify/internal/notify"
	"github.com/Nomadcxx/sysc-notify/protocol"
)

func TestOwnerSequencesAddAndReplacement(t *testing.T) {
	clock := newManualClock()
	sink := newEventSink()
	owner := Start(clock, sink)
	t.Cleanup(func() { _ = owner.Close() })

	first := do(t, owner, Command{Kind: Add, Candidate: candidate("first", 10*time.Second)})
	replacement := candidate("replacement", 20*time.Second)
	replacement.ReplacesID = first.ID
	second := do(t, owner, Command{Kind: Add, Candidate: replacement})
	if !second.Replaced || second.ID != first.ID {
		t.Fatalf("replacement result = %#v, first = %#v", second, first)
	}
	events := sink.Events()
	if len(events) != 2 || events[0].Sequence != 1 || events[1].Sequence != 2 {
		t.Fatalf("events = %#v", events)
	}
	if events[0].Delta == nil || events[0].Delta.Kind != protocol.DeltaAdded || events[1].Delta == nil || events[1].Delta.Kind != protocol.DeltaReplaced {
		t.Fatalf("delta kinds = %#v", events)
	}
	snapshot := snapshot(t, owner)
	if len(snapshot.Active) != 1 || snapshot.Active[0].Summary != "replacement" || len(snapshot.History) != 0 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestOwnerClosePathsUseTypedReasons(t *testing.T) {
	for name, tc := range map[string]struct {
		command CommandKind
		reason  protocol.CloseReason
	}{
		"requested": {command: CloseRequested, reason: protocol.CloseRequested},
		"dismissed": {command: Dismiss, reason: protocol.CloseDismissed},
	} {
		t.Run(name, func(t *testing.T) {
			owner, sink := startTestOwner(t)
			result := do(t, owner, Command{Kind: Add, Candidate: candidate("record", time.Minute)})
			do(t, owner, Command{Kind: tc.command, ID: result.ID})
			events := sink.Events()
			last := events[len(events)-1]
			if last.Delta == nil || last.Delta.CloseReason != tc.reason {
				t.Fatalf("last event = %#v", last)
			}
			if len(snapshot(t, owner).Active) != 0 {
				t.Fatal("closed record remains active")
			}
		})
	}
}

func TestOwnerActionPrecedesOptionalClose(t *testing.T) {
	owner, sink := startTestOwner(t)
	nonResident := candidate("ordinary", time.Minute)
	nonResident.Actions = []protocol.Action{{Key: "default", Label: "Open"}}
	id := do(t, owner, Command{Kind: Add, Candidate: nonResident}).ID
	do(t, owner, Command{Kind: InvokeAction, ID: id, ActionKey: "default"})
	events := sink.Events()
	if len(events) != 3 || events[1].Action == nil || events[2].Delta == nil || events[2].Delta.Kind != protocol.DeltaClosed {
		t.Fatalf("events = %#v", events)
	}

	resident := candidate("resident", time.Minute)
	resident.Resident = true
	resident.Actions = []protocol.Action{{Key: "keep", Label: "Keep"}}
	residentID := do(t, owner, Command{Kind: Add, Candidate: resident}).ID
	do(t, owner, Command{Kind: InvokeAction, ID: residentID, ActionKey: "keep"})
	if len(snapshot(t, owner).Active) != 1 {
		t.Fatal("resident action closed its notification")
	}
}

func TestOwnerReplyValidatesTextAndPrecedesClose(t *testing.T) {
	owner, sink := startTestOwner(t)
	record := candidate("reply", time.Minute)
	record.InlineReply = true
	id := do(t, owner, Command{Kind: Add, Candidate: record}).ID
	if _, err := owner.Do(context.Background(), Command{Kind: SubmitReply, ID: id, ReplyText: string([]byte{0xff})}); err == nil {
		t.Fatal("invalid reply text succeeded")
	}
	if !hasID(snapshot(t, owner), id) {
		t.Fatal("invalid reply removed notification")
	}
	do(t, owner, Command{Kind: SubmitReply, ID: id, ReplyText: "answer"})
	events := sink.Events()
	if len(events) != 3 || events[1].Reply == nil || events[1].Reply.Text != "answer" || events[2].Delta == nil {
		t.Fatalf("events = %#v", events)
	}
}

func TestOwnerCapacityEvictsFiniteNonCriticalFirst(t *testing.T) {
	owner, sink := startTestOwner(t)
	critical := candidate("critical", 0)
	critical.Urgency = protocol.UrgencyCritical
	criticalID := do(t, owner, Command{Kind: Add, Candidate: critical}).ID
	finiteID := do(t, owner, Command{Kind: Add, Candidate: candidate("finite", time.Hour)}).ID
	for i := 2; i < protocol.MaxActiveNotifications; i++ {
		persistent := candidate("persistent", 0)
		do(t, owner, Command{Kind: Add, Candidate: persistent})
	}
	do(t, owner, Command{Kind: Add, Candidate: candidate("overflow", time.Hour)})

	snapshot := snapshot(t, owner)
	if len(snapshot.Active) != protocol.MaxActiveNotifications || hasID(snapshot, finiteID) || !hasID(snapshot, criticalID) {
		t.Fatalf("capacity snapshot has finite=%v critical=%v len=%d", hasID(snapshot, finiteID), hasID(snapshot, criticalID), len(snapshot.Active))
	}
	events := sink.Events()
	found := false
	for _, event := range events {
		if event.Delta != nil && event.Delta.Kind == protocol.DeltaClosed && event.Delta.ID == finiteID && event.Delta.CloseReason == protocol.CloseUndefined {
			found = true
		}
	}
	if !found {
		t.Fatalf("no capacity close event for %d", finiteID)
	}
}

func TestOwnerRejectsInvalidReplacementWithoutMutation(t *testing.T) {
	owner, _ := startTestOwner(t)
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("old", time.Minute)}).ID
	invalid := candidate("new", time.Minute)
	invalid.ReplacesID = id
	invalid.Actions = make([]protocol.Action, protocol.MaxActionPairs+1)
	if _, err := owner.Do(context.Background(), Command{Kind: Add, Candidate: invalid}); err == nil {
		t.Fatal("invalid replacement succeeded")
	}
	got := snapshot(t, owner)
	if len(got.Active) != 1 || got.Active[0].Summary != "old" {
		t.Fatalf("snapshot after invalid replacement = %#v", got)
	}
}

func startTestOwner(t *testing.T) (*Owner, *eventSink) {
	t.Helper()
	sink := newEventSink()
	owner := Start(newManualClock(), sink)
	t.Cleanup(func() { _ = owner.Close() })
	return owner, sink
}

func candidate(summary string, timeout time.Duration) notify.Candidate {
	return notify.Candidate{Summary: summary, Urgency: protocol.UrgencyNormal, ExpireTimeout: int32(timeout / time.Millisecond)}
}

func do(t *testing.T, owner *Owner, command Command) Result {
	t.Helper()
	result, err := owner.Do(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func snapshot(t *testing.T, owner *Owner) protocol.Snapshot {
	t.Helper()
	snapshot, err := owner.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func hasID(snapshot protocol.Snapshot, id uint32) bool {
	for _, record := range snapshot.Active {
		if record.ID == id {
			return true
		}
	}
	return false
}

type eventSink struct {
	mu     sync.Mutex
	events []Event
}

func newEventSink() *eventSink { return &eventSink{} }

func (s *eventSink) Publish(event Event) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return true
}

func (s *eventSink) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}
