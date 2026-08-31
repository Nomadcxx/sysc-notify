package history

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInvalidHistoryIsQuarantinedAndNeverOverwritten(t *testing.T) {
	for name, contents := range map[string]string{
		"corrupt": `{not-json`,
		"future":  `{"version":2,"entries":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			stateHome := t.TempDir()
			dir := filepath.Join(stateHome, "sysc-notify")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			historyPath := filepath.Join(dir, historyFilename)
			if err := os.WriteFile(historyPath, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := OpenAt(stateHome, time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatal(err)
			}
			if len(store.Entries()) != 0 {
				t.Fatalf("invalid history loaded: %#v", store.Entries())
			}
			matches, err := filepath.Glob(filepath.Join(dir, "history.quarantine-*.json"))
			if err != nil || len(matches) != 1 {
				t.Fatalf("quarantine matches = %v, %v", matches, err)
			}
			got, err := os.ReadFile(matches[0])
			if err != nil || string(got) != contents {
				t.Fatalf("quarantine contents = %q, %v", got, err)
			}
			if _, err := os.Stat(historyPath); !os.IsNotExist(err) {
				t.Fatalf("invalid history path remains: %v", err)
			}
			if _, _, err := store.Add(testEntry(1, time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)), time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(matches[0])
			if err != nil || string(after) != contents {
				t.Fatalf("quarantine was overwritten = %q, %v", after, err)
			}
		})
	}
}

func TestOrphansWaitForValidCommitAndInterruptedTempIsIgnored(t *testing.T) {
	stateHome := t.TempDir()
	dir := filepath.Join(stateHome, "sysc-notify")
	images := filepath.Join(dir, "images")
	if err := os.MkdirAll(images, 0o700); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(images, "orphan.png")
	if err := os.WriteFile(orphan, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, historyFilename), []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("orphan removed without a valid committed reference set: %v", err)
	}
	if _, _, err := store.Add(testEntry(7, now), now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan remains after valid commit: %v", err)
	}
	committed, err := os.ReadFile(filepath.Join(dir, historyFilename))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".history.json.tmp-interrupted"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.Entries(); len(got) != 1 || got[0].ID != 7 {
		t.Fatalf("interrupted write replaced committed history: %#v", got)
	}
	after, err := os.ReadFile(filepath.Join(dir, historyFilename))
	if err != nil || string(after) != string(committed) {
		t.Fatalf("committed history changed: %v", err)
	}
}
