package history

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// unwritableStateDir makes the state directory reject new files, the way a
// full disk or a changed permission does, and returns a function that makes
// it writable again.
func unwritableStateDir(t *testing.T, stateHome string) func() {
	t.Helper()
	dir := filepath.Join(stateHome, "sysc-notify")
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	restore := func() {
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatalf("restore the state directory: %v", err)
		}
	}
	t.Cleanup(restore)
	return restore
}

// A commit that failed is not a durable one. The store must keep the snapshot
// pending, so the close writes it instead of dropping the entries that were
// added while the disk was unusable.
func TestAFailedPersistIsRetriedByClose(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Add(testEntry(1, now), now); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("seed flush: %v", err)
	}

	restore := unwritableStateDir(t, stateHome)
	if _, _, err := store.Add(testEntry(2, now), now); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(); err == nil {
		t.Fatal("Flush did not report the failed persist")
	}

	// The disk works again. The close must write what the failed attempt
	// left pending.
	restore()
	if err := store.Close(); err != nil {
		t.Fatalf("close after a failed persist: %v", err)
	}

	reopened, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	ids := reopened.IDs()
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("reopened ids = %v, want [1 2]", ids)
	}
}

// The worker keeps trying on its own: a disk that comes back must not need a
// new notification to write the entries that were pending.
func TestAFailedPersistIsRetriedByTheWorker(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, _, err := store.Add(testEntry(1, now), now); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("seed flush: %v", err)
	}

	restore := unwritableStateDir(t, stateHome)
	if _, _, err := store.Add(testEntry(2, now), now); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(); err == nil {
		t.Fatal("Flush did not report the failed persist")
	}
	restore()

	deadline := time.Now().Add(10 * time.Second)
	for {
		reopened, err := OpenAt(stateHome, now)
		if err != nil {
			t.Fatal(err)
		}
		ids := reopened.IDs()
		_ = reopened.Close()
		if len(ids) == 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the worker did not retry the failed persist: ids = %v", ids)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
