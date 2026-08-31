package history

import (
	"bytes"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

func TestStorePersistsVersionedHistory(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 10, 15, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	entry := protocol.HistoryEntry{
		ID: 42, AppName: "Firefox", Summary: "Done", Urgency: protocol.UrgencyNormal, Timestamp: now,
	}
	if _, _, err := store.Add(entry, now); err != nil {
		t.Fatal(err)
	}

	contents, err := os.ReadFile(filepath.Join(stateHome, "sysc-notify", "history.json"))
	if err != nil {
		t.Fatal(err)
	}
	var header struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(contents, &header); err != nil {
		t.Fatal(err)
	}
	if header.Version != 1 {
		t.Fatalf("schema version = %d, want 1", header.Version)
	}
	for path, want := range map[string]os.FileMode{
		filepath.Join(stateHome, "sysc-notify"):                 0o700,
		filepath.Join(stateHome, "sysc-notify", "images"):       0o700,
		filepath.Join(stateHome, "sysc-notify", "history.json"): 0o600,
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Fatalf("mode %s = %o, want %o", path, got, want)
		}
	}

	reopened, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	got := reopened.Entries()
	if len(got) != 1 || got[0].ID != 42 || got[0].Summary != "Done" {
		t.Fatalf("reopened entries = %#v", got)
	}
}

func TestOpenPersistsRetentionSweep(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Add(testEntry(1, now), now); err != nil {
		t.Fatal(err)
	}
	later := now.Add(protocol.HistoryRetention + time.Nanosecond)
	reopened, err := OpenAt(stateHome, later)
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.Entries()) != 0 {
		t.Fatalf("expired history loaded: %#v", reopened.Entries())
	}
	again, err := OpenAt(stateHome, later)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Entries()) != 0 {
		t.Fatalf("startup sweep was not persisted: %#v", again.Entries())
	}
}

func TestStoreBoundsAndSweepsHistory(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	for id := uint32(1); id <= protocol.MaxHistoryEntries+1; id++ {
		entry := testEntry(id, now.Add(time.Duration(id)*time.Second))
		_, removed, err := store.Add(entry, now)
		if err != nil {
			t.Fatal(err)
		}
		if id == protocol.MaxHistoryEntries+1 && !slices.Equal(removed, []uint32{1}) {
			t.Fatalf("capacity removals = %v, want [1]", removed)
		}
	}
	if got := store.Entries(); len(got) != protocol.MaxHistoryEntries || got[0].ID != 2 {
		t.Fatalf("bounded entries: len=%d first=%d", len(got), got[0].ID)
	}

	removed, err := store.Sweep(now.Add(protocol.HistoryRetention + 2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != protocol.MaxHistoryEntries || len(store.Entries()) != 0 {
		t.Fatalf("sweep removed=%d remaining=%d", len(removed), len(store.Entries()))
	}
	reopened, err := OpenAt(stateHome, now.Add(protocol.HistoryRetention+2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.Entries()) != 0 {
		t.Fatalf("swept entries returned after restart: %#v", reopened.Entries())
	}
}

func TestSeenAndClearAreIdempotent(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	for id := uint32(1); id <= 2; id++ {
		if _, _, err := store.Add(testEntry(id, now), now); err != nil {
			t.Fatal(err)
		}
	}
	changed, err := store.MarkSeen([]uint32{2, 1, 2, 99})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(changed, []uint32{1, 2}) {
		t.Fatalf("seen changes = %v, want [1 2]", changed)
	}
	changed, err = store.MarkSeen([]uint32{1, 2})
	if err != nil || len(changed) != 0 {
		t.Fatalf("idempotent mark seen = %v, %v", changed, err)
	}
	reopened, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.Entries(); len(got) != 2 || !got[0].Seen || !got[1].Seen {
		t.Fatalf("seen state after restart = %#v", got)
	}
	cleared, err := reopened.Clear()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cleared, []uint32{1, 2}) || len(reopened.Entries()) != 0 {
		t.Fatalf("clear = %v, entries=%#v", cleared, reopened.Entries())
	}
	cleared, err = reopened.Clear()
	if err != nil || len(cleared) != 0 {
		t.Fatalf("idempotent clear = %v, %v", cleared, err)
	}
}

func TestStoreDownscalesAndReusesContentAddressedImage(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	entry := testEntry(1, now)
	entry.Image = testPNG(t, 192, 96)
	added, _, err := store.Add(entry, now)
	if err != nil {
		t.Fatal(err)
	}
	if added.Image == nil || added.Image.Width != 96 || added.Image.Height != 48 {
		t.Fatalf("stored image = %#v", added.Image)
	}
	images, err := filepath.Glob(filepath.Join(stateHome, "sysc-notify", "images", "*.png"))
	if err != nil || len(images) != 1 {
		t.Fatalf("image sidecars = %v, %v", images, err)
	}
	before, err := os.Stat(images[0])
	if err != nil {
		t.Fatal(err)
	}
	entry.ID = 2
	if _, _, err := store.Add(entry, now); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(images[0])
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("content-addressed image was rewritten: %s -> %s", before.ModTime(), after.ModTime())
	}
	reopened, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	got := reopened.Entries()
	if len(got) != 2 || got[0].Image == nil || !bytes.Equal(got[0].Image.Data, added.Image.Data) {
		t.Fatalf("reopened image entries = %#v", got)
	}
}

func TestStoreRejectsSymlinkPath(t *testing.T) {
	root := t.TempDir()
	realState := filepath.Join(root, "real")
	if err := os.Mkdir(realState, 0o700); err != nil {
		t.Fatal(err)
	}
	linkedState := filepath.Join(root, "linked")
	if err := os.Symlink(realState, linkedState); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenAt(linkedState, time.Now()); err == nil {
		t.Fatal("symlink state path succeeded")
	}
}

func testEntry(id uint32, timestamp time.Time) protocol.HistoryEntry {
	return protocol.HistoryEntry{ID: id, Summary: "record", Urgency: protocol.UrgencyNormal, Timestamp: timestamp}
}

func testPNG(t *testing.T, width, height int) *protocol.Image {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.SetNRGBA(x, y, color.NRGBA{R: uint8(x), G: uint8(y), B: 0x55, A: 0xff})
		}
	}
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, img); err != nil {
		t.Fatal(err)
	}
	return &protocol.Image{MediaType: "image/png", Width: uint32(width), Height: uint32(height), Data: encoded.Bytes()}
}
