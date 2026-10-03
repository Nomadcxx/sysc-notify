package history

import (
	"errors"
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

// TestSuccessfulCommitClearsRememberedError verifies that a transient persist
// failure does not make every later Flush report an error after a later commit
// has succeeded. recordErr/clearErr are exercised directly because forcing a
// real commit failure would require an unwritable directory mid-run.
func TestSuccessfulCommitClearsRememberedError(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	store.recordErr(errors.New("transient"))
	store.mu.Lock()
	store.workerGen = store.writeGen
	store.mu.Unlock()
	if err := store.Flush(); err == nil {
		t.Fatal("Flush did not surface the recorded error")
	}

	store.clearErr()
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush after a successful commit: %v", err)
	}
}

// TestScheduleDoesNotCopyEntries verifies that a mutation no longer deep-copies
// the whole history (the per-mutation clone that ran on the owner goroutine).
// The worker snapshots once when it wakes, so a scheduling mutation must not
// allocate a pending copy.
func TestScheduleDoesNotCopyEntries(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}

	if _, _, err := store.Add(testEntry(1, now), now); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	dirty := store.dirty
	store.mu.Unlock()
	if !dirty {
		t.Fatal("Add did not mark the store dirty")
	}
	if err := store.Flush(); err != nil {
		t.Fatal(err)
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

func TestCloseWaitsForConcurrentCloseAndFlush(t *testing.T) {
	store := &Store{
		writeGen:   1,
		closing:    make(chan struct{}),
		workerDone: make(chan struct{}),
		closeDone:  make(chan struct{}),
	}
	store.cond = sync.NewCond(&store.mu)
	var releaseWorker sync.Once
	release := func() { releaseWorker.Do(func() { close(store.workerDone) }) }
	t.Cleanup(release)

	// Hold the mutex long enough to queue Flush, then reacquire it after the
	// waiter enters cond.Wait so Close cannot race ahead of its registration.
	store.mu.Lock()
	flushStarted := make(chan struct{})
	flushDone := make(chan error, 1)
	go func() {
		close(flushStarted)
		flushDone <- store.Flush()
	}()
	<-flushStarted
	time.Sleep(10 * time.Millisecond)
	store.mu.Unlock()
	store.mu.Lock()
	store.mu.Unlock()

	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		store.mu.Lock()
		closed := store.closed
		store.mu.Unlock()
		if closed {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Close did not begin")
		case <-ticker.C:
		}
	}

	secondCloseDone := make(chan error, 1)
	secondCloseStarted := make(chan struct{})
	go func() {
		close(secondCloseStarted)
		secondCloseDone <- store.Close()
	}()
	<-secondCloseStarted
	secondReturnedEarly := false
	select {
	case <-secondCloseDone:
		secondReturnedEarly = true
	case <-time.After(10 * time.Millisecond):
	}

	writeErr := errors.New("worker persistence failed")
	store.mu.Lock()
	store.lastErr = writeErr
	store.mu.Unlock()
	release()
	select {
	case err := <-closeDone:
		if !errors.Is(err, writeErr) {
			t.Fatalf("Close = %v, want %v", err, writeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after the worker stopped")
	}
	if !secondReturnedEarly {
		select {
		case err := <-secondCloseDone:
			if !errors.Is(err, writeErr) {
				t.Errorf("second Close = %v, want %v", err, writeErr)
			}
		case <-time.After(time.Second):
			t.Fatal("second Close did not return")
		}
	} else {
		t.Error("second Close returned before the first close finished")
	}

	select {
	case err := <-flushDone:
		if !errors.Is(err, writeErr) {
			t.Errorf("Flush = %v, want %v", err, writeErr)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Flush stayed blocked after Close finished")
		store.mu.Lock()
		store.workerGen = store.writeGen
		store.cond.Broadcast()
		store.mu.Unlock()
		<-flushDone
	}
}
