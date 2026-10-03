package history

import (
	"sync"
	"testing"
	"time"
)

// TestAsyncPersistenceCoalescesBursts verifies that a burst of mutations is
// visible in memory immediately, that a single Flush makes the whole burst
// durable, and that the on-disk state matches what an immediate reopen reads.
func TestAsyncPersistenceCoalescesBursts(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	const burst = 40
	for id := uint32(1); id <= burst; id++ {
		if _, _, err := store.Add(testEntry(id, now), now); err != nil {
			t.Fatalf("add %d: %v", id, err)
		}
	}
	// In-memory state is authoritative and complete before any disk write.
	if got := store.Entries(); len(got) != burst {
		t.Fatalf("in-memory entries = %d, want %d", len(got), burst)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	reopened, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if got := reopened.Entries(); len(got) != burst {
		t.Fatalf("reopened entries = %d, want %d", len(got), burst)
	}
	for i, entry := range reopened.Entries() {
		if entry.ID != uint32(i+1) {
			t.Fatalf("reopened entry %d = id %d", i, entry.ID)
		}
	}
}

// TestClosePersistsPendingWork verifies Close commits mutations that were
// never explicitly flushed (the durable-close path an app shutdown relies on).
func TestClosePersistsPendingWork(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Add(testEntry(9, now), now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remove([]uint32{7}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if got := reopened.Entries(); len(got) != 1 || got[0].ID != 9 {
		t.Fatalf("reopened entries = %#v, want [9]", got)
	}
}

// TestConcurrentMutationsFlush is a race probe: mutations must be safe to
// issue from other goroutines while the persistence worker commits and while
// Flush drains in-flight work.
func TestConcurrentMutationsFlush(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(base uint32) {
			defer wg.Done()
			for i := uint32(0); i < 12; i++ {
				id := base*1000 + i
				if _, _, err := store.Add(testEntry(id, now), now); err != nil {
					t.Errorf("add %d: %v", id, err)
					return
				}
				_, _ = store.MarkSeen([]uint32{id})
			}
		}(uint32(worker + 1))
	}
	wg.Wait()
	if err := store.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	reopened, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if got := reopened.Entries(); len(got) != 48 {
		t.Fatalf("reopened entries = %d, want 48", len(got))
	}
	for _, entry := range reopened.Entries() {
		if !entry.Seen {
			t.Fatalf("entry %d not marked seen", entry.ID)
		}
	}
	ids := make(map[uint32]struct{}, len(reopened.Entries()))
	for _, entry := range reopened.Entries() {
		ids[entry.ID] = struct{}{}
	}
	for base := uint32(1); base <= 4; base++ {
		for i := uint32(0); i < 12; i++ {
			id := base*1000 + i
			if _, ok := ids[id]; !ok {
				t.Fatalf("id %d missing after reopen", id)
			}
		}
	}
}
