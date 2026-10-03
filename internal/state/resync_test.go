package state

import (
	"context"
	"testing"
	"time"
)

// A PresenterResync publishes a snapshot on the owner goroutine without
// advancing the sequence, so a presenter can rebase mid-stream. The snapshot
// must reflect everything published before it and be scoped to the requesting
// generation.
func TestPresenterResyncPublishesSnapshotWithoutAdvancingSequence(t *testing.T) {
	owner, sink := startTestOwner(t)
	before := snapshot(t, owner)
	id := do(t, owner, Command{Kind: Add, Candidate: candidate("resync", time.Minute)}).ID
	afterAdd := snapshot(t, owner)

	do(t, owner, Command{Kind: PresenterResync, Generation: 7})

	events := sink.Events()
	var resync *Event
	for i := range events {
		if events[i].Snapshot != nil {
			resync = &events[i]
		}
	}
	if resync == nil {
		t.Fatal("PresenterResync published no snapshot")
	}
	if resync.Generation != 7 {
		t.Fatalf("resync generation = %d, want 7", resync.Generation)
	}
	if resync.Sequence != afterAdd.Sequence {
		t.Fatalf("resync sequence = %d, want %d", resync.Sequence, afterAdd.Sequence)
	}
	if len(resync.Snapshot.Active) != 1 || resync.Snapshot.Active[0].ID != id {
		t.Fatalf("resync snapshot = %#v, want active %d", resync.Snapshot, id)
	}
	if got := snapshot(t, owner); got.Sequence != before.Sequence+1 {
		t.Fatalf("resync advanced sequence to %d, want %d", got.Sequence, before.Sequence+1)
	}
}

// The snapshot a resync carries must be a valid baseline: a delta published
// immediately after it is exactly sequence+1, so the shell client accepts it.
func TestPresenterResyncBaselineIsContiguous(t *testing.T) {
	owner, sink := startTestOwner(t)
	do(t, owner, Command{Kind: Add, Candidate: candidate("first", time.Minute)})
	do(t, owner, Command{Kind: PresenterResync, Generation: 1})
	do(t, owner, Command{Kind: Add, Candidate: candidate("second", time.Minute)})

	var baseline *Event
	var following *Event
	for i := range sink.Events() {
		event := sink.Events()[i]
		if event.Snapshot != nil {
			baseline = &event
			continue
		}
		if baseline != nil && event.Delta != nil {
			following = &event
			break
		}
	}
	if baseline == nil {
		t.Fatal("no resync snapshot")
	}
	if following == nil {
		t.Fatal("no delta followed the resync snapshot")
	}
	if following.Sequence != baseline.Sequence+1 {
		t.Fatalf("delta sequence = %d, want %d", following.Sequence, baseline.Sequence+1)
	}
}

func TestPresenterResyncIsIdempotent(t *testing.T) {
	owner, sink := startTestOwner(t)
	do(t, owner, Command{Kind: Add, Candidate: candidate("x", time.Minute)})
	for range 3 {
		if _, err := owner.Do(context.Background(), Command{Kind: PresenterResync, Generation: 3}); err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	for _, event := range sink.Events() {
		if event.Snapshot != nil {
			count++
		}
	}
	if count != 3 {
		t.Fatalf("resync snapshots = %d, want 3", count)
	}
}
