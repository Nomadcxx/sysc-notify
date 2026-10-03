package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Nomadcxx/sysc-notify/internal/history"
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

func TestOwnerProducerReplacesAndClosesWithoutHistory(t *testing.T) {
	clock := newManualClock()
	store, err := history.OpenAt(t.TempDir(), clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	sink := newEventSink()
	owner := StartWithHistory(clock, sink, store)
	t.Cleanup(func() { _ = owner.Close() })

	first := do(t, owner, Command{Kind: ProducerPublish, Producer: producerRequest(15)})
	if first.ID == 0 || first.Replaced {
		t.Fatalf("first producer result = %#v", first)
	}
	active := snapshot(t, owner)
	if len(active.Active) != 1 || active.Active[0].ID != first.ID || active.Active[0].ExpireTimeoutMS != 0 || active.Active[0].Value == nil || *active.Active[0].Value != 15 {
		t.Fatalf("producer snapshot = %#v", active)
	}

	replacement := producerRequest(10)
	replaced := do(t, owner, Command{Kind: ProducerPublish, Producer: replacement})
	if replaced.ID != first.ID || !replaced.Replaced {
		t.Fatalf("replacement result = %#v, first = %#v", replaced, first)
	}
	if active := snapshot(t, owner); len(active.Active) != 1 || active.Active[0].Value == nil || *active.Active[0].Value != 10 {
		t.Fatalf("replacement snapshot = %#v", active)
	}

	closed := do(t, owner, Command{Kind: ProducerClose, Producer: &protocol.ProducerRequest{Key: "sysc-shell:battery-low"}})
	if closed.ID != first.ID || len(snapshot(t, owner).Active) != 0 || len(snapshot(t, owner).History) != 0 {
		t.Fatalf("close result or snapshot = %#v", closed)
	}
	if got := do(t, owner, Command{Kind: ProducerClose, Producer: &protocol.ProducerRequest{Key: "sysc-shell:battery-low"}}); got.ID != 0 {
		t.Fatalf("idempotent close result = %#v", got)
	}

	next := do(t, owner, Command{Kind: ProducerPublish, Producer: producerRequest(5)})
	if next.ID == first.ID {
		t.Fatalf("reused closed producer ID %d", next.ID)
	}
	if events := sink.Events(); !hasDelta(events, protocol.DeltaReplaced) || !hasDelta(events, protocol.DeltaClosed) {
		t.Fatalf("producer deltas = %#v", events)
	}
}

func TestOwnerProducerMappingIsRemovedByDismissal(t *testing.T) {
	owner, _ := startTestOwner(t)
	first := do(t, owner, Command{Kind: ProducerPublish, Producer: producerRequest(15)})
	do(t, owner, Command{Kind: Dismiss, ID: first.ID})
	next := do(t, owner, Command{Kind: ProducerPublish, Producer: producerRequest(14)})
	if next.ID == first.ID || next.Replaced {
		t.Fatalf("producer after dismissal = %#v, first = %#v", next, first)
	}
}

func TestOwnerPersistsOnlyEligibleClosedHistory(t *testing.T) {
	clock := newManualClock()
	stateHome := t.TempDir()
	store, err := history.OpenAt(stateHome, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	owner := StartWithHistory(clock, newEventSink(), store)
	ordinary := candidate("ordinary", time.Minute)
	ordinary.AppName = "Browser"
	ordinary.Actions = []protocol.Action{{Key: "open", Label: "Open"}}
	ordinary.Sender = notify.Sender{Name: ":1.42", PID: 1234}
	ordinaryID := do(t, owner, Command{Kind: Add, Candidate: ordinary}).ID
	do(t, owner, Command{Kind: Dismiss, ID: ordinaryID})
	for _, ineligible := range []notify.Candidate{
		func() notify.Candidate { c := candidate("private", time.Minute); c.Private = true; return c }(),
		func() notify.Candidate { c := candidate("transient", time.Minute); c.Transient = true; return c }(),
	} {
		id := do(t, owner, Command{Kind: Add, Candidate: ineligible}).ID
		do(t, owner, Command{Kind: Dismiss, ID: id})
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := history.OpenAt(stateHome, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	restarted := StartWithHistory(clock, newEventSink(), reopened)
	t.Cleanup(func() { _ = restarted.Close() })
	got := snapshot(t, restarted)
	if len(got.Active) != 0 || len(got.History) != 1 {
		t.Fatalf("restart snapshot = %#v", got)
	}
	entry := got.History[0]
	if entry.ID != ordinaryID || entry.AppName != "Browser" || entry.Summary != "ordinary" {
		t.Fatalf("history entry = %#v", entry)
	}
}

func TestOwnerHistoryCommandsDoNotMutateActiveOrExistingHistory(t *testing.T) {
	clock := newManualClock()
	store, err := history.OpenAt(t.TempDir(), clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	sink := newEventSink()
	owner := StartWithHistory(clock, sink, store)
	t.Cleanup(func() { _ = owner.Close() })

	closedID := do(t, owner, Command{Kind: Add, Candidate: candidate("closed", time.Minute)}).ID
	do(t, owner, Command{Kind: Dismiss, ID: closedID})
	activeID := do(t, owner, Command{Kind: Add, Candidate: candidate("active", 0)}).ID
	do(t, owner, Command{Kind: HistoryMarkSeen, IDs: []uint32{closedID}})
	if got := snapshot(t, owner); len(got.History) != 1 || !got.History[0].Seen || !hasID(got, activeID) {
		t.Fatalf("mark seen snapshot = %#v", got)
	}
	do(t, owner, Command{Kind: HistoryClear})
	if got := snapshot(t, owner); len(got.History) != 0 || !hasID(got, activeID) {
		t.Fatalf("clear snapshot = %#v", got)
	}

	preservedID := do(t, owner, Command{Kind: Add, Candidate: candidate("preserved", time.Minute)}).ID
	do(t, owner, Command{Kind: Dismiss, ID: preservedID})
	do(t, owner, Command{Kind: DismissAll})
	got := snapshot(t, owner)
	if len(got.Active) != 0 || !historyHasID(got, preservedID) {
		t.Fatalf("dismiss all snapshot = %#v", got)
	}
	events := sink.Events()
	seen, cleared := false, false
	for _, event := range events {
		if event.Delta == nil {
			continue
		}
		seen = seen || event.Delta.Kind == protocol.DeltaHistorySeen
		cleared = cleared || event.Delta.Kind == protocol.DeltaHistoryCleared
	}
	if !seen || !cleared {
		t.Fatalf("history command events missing: %#v", events)
	}
}

func TestHistoryRemovePublishesOneDeltaPerEntry(t *testing.T) {
	clock := newManualClock()
	store, err := history.OpenAt(t.TempDir(), clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	sink := newEventSink()
	owner := StartWithHistory(clock, sink, store)
	t.Cleanup(func() { _ = owner.Close() })

	var ids []uint32
	for _, name := range []string{"one", "two", "three"} {
		id := do(t, owner, Command{Kind: Add, Candidate: candidate(name, time.Minute)}).ID
		do(t, owner, Command{Kind: Dismiss, ID: id})
		ids = append(ids, id)
	}

	do(t, owner, Command{Kind: HistoryRemove, IDs: []uint32{ids[0], ids[2]}})

	var removed []uint32
	for _, event := range sink.Events() {
		if event.Delta != nil && event.Delta.Kind == protocol.DeltaHistoryRemoved {
			removed = append(removed, event.Delta.ID)
		}
	}
	if !slices.Equal(removed, []uint32{ids[0], ids[2]}) {
		t.Fatalf("deltas = %v, want %v", removed, []uint32{ids[0], ids[2]})
	}
}

func TestOwnerSweepsHistoryEveryMinute(t *testing.T) {
	clock := newManualClock()
	store, err := history.OpenAt(t.TempDir(), clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	owner := StartWithHistory(clock, newEventSink(), store)
	t.Cleanup(func() { _ = owner.Close() })
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("old", time.Minute)}).ID
	do(t, owner, Command{Kind: Dismiss, ID: id})
	clock.Advance(protocol.HistoryRetention + time.Minute)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(snapshot(t, owner).History) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("expired history was not swept")
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

func TestInlineReplyActionCanBeRepliedTo(t *testing.T) {
	owner, sink := startTestOwner(t)
	candidate, err := notify.Normalize(notify.Request{
		Summary: "Message from Alice",
		Actions: []string{"inline-reply", "Reply"},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := do(t, owner, Command{Kind: Add, Candidate: candidate}).ID
	do(t, owner, Command{Kind: SubmitReply, ID: id, ReplyText: "hello"})
	events := sink.Events()
	if len(events) < 2 || events[1].Reply == nil || events[1].Reply.Text != "hello" {
		t.Fatalf("events = %#v", events)
	}
}

func TestReplyPlaceholderIsOnTheNotification(t *testing.T) {
	owner, _ := startTestOwner(t)
	candidate, err := notify.Normalize(notify.Request{
		Summary: "Message from Alice",
		Actions: []string{"inline-reply", "Reply"},
		Hints:   map[string]any{notify.HintInlineReplyPlaceholder: "Reply to Alice"},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := do(t, owner, Command{Kind: Add, Candidate: candidate}).ID
	got := snapshot(t, owner)
	if len(got.Active) != 1 || got.Active[0].ID != id {
		t.Fatalf("snapshot = %#v", got)
	}
	notification := got.Active[0]
	if !notification.InlineReply || notification.ReplyPlaceholder != "Reply to Alice" {
		t.Fatalf("notification = %#v", notification)
	}
}

func TestStalePresenterCommandsDoNotMutate(t *testing.T) {
	clock := newManualClock()
	store, err := history.OpenAt(t.TempDir(), clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	sink := newEventSink()
	owner := StartWithHistory(clock, sink, store)
	t.Cleanup(func() { _ = owner.Close() })

	closedID := do(t, owner, Command{Kind: Add, Candidate: candidate("closed", time.Minute)}).ID
	do(t, owner, Command{Kind: Dismiss, ID: closedID})
	live := candidate("live", time.Minute)
	live.Resident = true
	live.InlineReply = true
	live.Actions = []protocol.Action{{Key: "open", Label: "Open"}}
	liveID := do(t, owner, Command{Kind: Add, Candidate: live}).ID
	do(t, owner, Command{
		Kind: PresentationRenew, Generation: 2,
		Presentations: []protocol.Presentation{{ID: liveID, State: protocol.PresentationQueued}},
	})
	producer := do(t, owner, Command{Kind: ProducerPublish, Producer: &protocol.ProducerRequest{
		Key: "sysc-shell:battery-low", Summary: "Battery low",
	}})

	stale := []Command{
		{Kind: Dismiss, ID: liveID, Generation: 1},
		{Kind: InvokeAction, ID: liveID, ActionKey: "open", Generation: 1},
		{Kind: SubmitReply, ID: liveID, ReplyText: "nope", Generation: 1},
		{Kind: DismissAll, Generation: 1},
		{Kind: HistoryClear, Generation: 1},
		{Kind: HistoryRemove, IDs: []uint32{closedID}, Generation: 1},
		{Kind: HistoryMarkSeen, IDs: []uint32{closedID}, Generation: 1},
		{Kind: ProducerPublish, Generation: 1, Producer: &protocol.ProducerRequest{Key: "sysc-shell:battery-full", Summary: "Full"}},
		{Kind: ProducerClose, Generation: 1, Producer: &protocol.ProducerRequest{Key: "sysc-shell:battery-low"}},
	}
	before := len(sink.Events())
	for _, command := range stale {
		_, err := owner.Do(context.Background(), command)
		if !errors.Is(err, ErrStale) {
			t.Fatalf("%v error = %v, want ErrStale", command.Kind, err)
		}
	}
	if got := len(sink.Events()); got != before {
		t.Fatalf("stale commands published %d events", got-before)
	}
	got := snapshot(t, owner)
	if !hasID(got, liveID) || !hasID(got, producer.ID) || !historyHasID(got, closedID) {
		t.Fatalf("snapshot after stale commands = %#v", got)
	}
	for _, entry := range got.History {
		if entry.ID == closedID && entry.Seen {
			t.Fatal("stale mark-seen changed history")
		}
	}

	do(t, owner, Command{Kind: InvokeAction, ID: liveID, ActionKey: "open", Generation: 2})
	do(t, owner, Command{Kind: HistoryMarkSeen, IDs: []uint32{closedID}, Generation: 2})
	do(t, owner, Command{Kind: Dismiss, ID: liveID})
	if hasID(snapshot(t, owner), liveID) {
		t.Fatal("ungenerated service dismiss was rejected")
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

func historyHasID(snapshot protocol.Snapshot, id uint32) bool {
	for _, record := range snapshot.History {
		if record.ID == id {
			return true
		}
	}
	return false
}

func producerRequest(value int32) *protocol.ProducerRequest {
	return &protocol.ProducerRequest{
		Key: "sysc-shell:battery-low", AppName: "sysc-shell", Summary: "Battery low",
		Body: "Battery is low.", Urgency: protocol.UrgencyCritical, Value: &value,
	}
}

func hasDelta(events []Event, kind protocol.DeltaKind) bool {
	for _, event := range events {
		if event.Delta != nil && event.Delta.Kind == kind {
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

func TestOwnerRestartDoesNotReuseHistoryIDs(t *testing.T) {
	clock := newManualClock()
	stateHome := t.TempDir()
	store, err := history.OpenAt(stateHome, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	owner := StartWithHistory(clock, newEventSink(), store)
	first := do(t, owner, Command{Kind: Add, Candidate: candidate("before restart", time.Minute)}).ID
	do(t, owner, Command{Kind: Dismiss, ID: first})
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := history.OpenAt(stateHome, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	restarted := StartWithHistory(clock, newEventSink(), reopened)
	t.Cleanup(func() { _ = restarted.Close() })
	second := do(t, restarted, Command{Kind: Add, Candidate: candidate("after restart", time.Minute)}).ID
	if second <= first {
		t.Fatalf("restarted owner allocated %d, history holds %d", second, first)
	}
	do(t, restarted, Command{Kind: Dismiss, ID: second})
	got := snapshot(t, restarted)
	if !historyHasID(got, first) || !historyHasID(got, second) {
		t.Fatalf("history = %#v, want ids %d and %d", got.History, first, second)
	}
}

func TestOwnerCloseCompletesWhenHistoryWriteFails(t *testing.T) {
	clock := newManualClock()
	stateHome := t.TempDir()
	store, err := history.OpenAt(stateHome, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	sink := newEventSink()
	owner := StartWithHistory(clock, sink, store)
	t.Cleanup(func() { _ = owner.Close() })
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("unpersisted", time.Minute)}).ID
	dir := filepath.Join(stateHome, "sysc-notify")
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	if _, err := owner.Do(context.Background(), Command{Kind: Dismiss, ID: id}); err != nil {
		t.Fatalf("dismiss with failing history = %v", err)
	}
	// Persistence is async: the failed write is recorded in memory and
	// reported (and logged) by the background commit, so snapshots and
	// deltas still reflect the entry; restarting without a working disk
	// loses it. Close must not hang on the failure.
	got := snapshot(t, owner)
	if !historyHasID(got, id) {
		t.Fatalf("snapshot history = %#v, want id %d retained in memory", got.History, id)
	}
	if !hasDelta(sink.Events(), protocol.DeltaClosed) || !hasDelta(sink.Events(), protocol.DeltaHistoryAdded) {
		t.Fatalf("events = %#v", sink.Events())
	}
	if err := owner.Close(); err == nil {
		t.Fatalf("close with failing history persistence = %v, want error", err)
	}
}

func TestOwnerHistoryRetentionCountsFromClose(t *testing.T) {
	clock := newManualClock()
	store, err := history.OpenAt(t.TempDir(), clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	owner := StartWithHistory(clock, newEventSink(), store)
	t.Cleanup(func() { _ = owner.Close() })
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("persistent", 0)}).ID
	clock.Advance(protocol.HistoryRetention + 24*time.Hour)
	do(t, owner, Command{Kind: Dismiss, ID: id})
	got := snapshot(t, owner)
	if !historyHasID(got, id) {
		t.Fatalf("history = %#v, want id %d", got.History, id)
	}
	if entry := got.History[len(got.History)-1]; !entry.Timestamp.Equal(clock.Now()) {
		t.Fatalf("history timestamp = %v, want close time %v", entry.Timestamp, clock.Now())
	}
}
