package history

import (
	"bytes"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

// distinctPNG encodes an image whose pixels vary with the id, so every entry
// owns a different sidecar without touching encoded PNG bytes.
func distinctPNG(tb testing.TB, id uint32) *protocol.Image {
	tb.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, 8, 8))
	for y := 0; y < 8; y++ {
		for x := 0; x < 8; x++ {
			img.SetNRGBA(x, y, color.NRGBA{R: byte(uint32(x) + id), G: byte(uint32(3*y) + id), B: 0x55, A: 0xff})
		}
	}
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, img); err != nil {
		tb.Fatal(err)
	}
	return &protocol.Image{MediaType: "image/png", Width: 8, Height: 8, Data: encoded.Bytes()}
}

// imageStore opens a store and fills it with entries that carry distinct
// images, then flushes so every sidecar is on disk.
func imageStore(t *testing.T, stateHome string, now time.Time, ids []uint32) (*Store, map[uint32]*protocol.Image) {
	t.Helper()
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	images := make(map[uint32]*protocol.Image, len(ids))
	for _, id := range ids {
		entry := testEntry(id, now)
		image := distinctPNG(t, id)
		images[id] = image
		prepared, err := PrepareImage(image)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.AddPrepared(entry, prepared, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}
	return store, images
}

// TestCommitNeverReadsAStoredSidecar pins the steady-state cost of a commit:
// a sidecar this process already placed is left alone. Reading, hashing and
// decoding every retained sidecar on every commit was issue #61.
func TestCommitNeverReadsAStoredSidecar(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 10, 15, 0, 0, time.UTC)
	ids := []uint32{1, 2, 3}
	store, _ := imageStore(t, stateHome, now, ids)

	reads := 0
	realRead := readSidecar
	readSidecar = func(path string, limit int64) ([]byte, error) {
		reads++
		return realRead(path, limit)
	}
	t.Cleanup(func() { readSidecar = realRead })

	if err := store.commit(cloneRetained(store.entries)); err != nil {
		t.Fatal(err)
	}
	if reads != 0 {
		t.Fatalf("a steady-state commit read %d sidecars, want 0", reads)
	}
}

// TestCommitRewritesASidecarRemovedExternally keeps the cheap guarantee: the
// stat says the sidecar is gone, so the next commit writes it again.
func TestCommitRewritesASidecarRemovedExternally(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 10, 15, 0, 0, time.UTC)
	store, images := imageStore(t, stateHome, now, []uint32{7})

	meta := store.entries[0].meta
	sidecar := filepath.Join(stateHome, "sysc-notify", "images", meta.digest+".png")
	if err := os.Remove(sidecar); err != nil {
		t.Fatal(err)
	}
	if err := store.commit(cloneRetained(store.entries)); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatalf("the removed sidecar was not rewritten: %v", err)
	}
	if !bytes.Equal(contents, images[7].Data) && !bytes.Equal(contents, store.entries[0].entry.Image.Data) {
		t.Fatal("the rewritten sidecar is not the retained image")
	}
}

// TestTwoEntriesShareOneSidecar covers the deduplicated case: the same
// prepared image under two entries names one sidecar, and cleanup keeps it
// while any entry still references it.
func TestTwoEntriesShareOneSidecar(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 10, 15, 0, 0, time.UTC)
	store, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	prepared, err := PrepareImage(distinctPNG(t, 4))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint32{1, 2} {
		if _, _, err := store.AddPrepared(testEntry(id, now), prepared, now); err != nil {
			t.Fatal(err)
		}
	}
	if store.entries[0].meta != store.entries[1].meta {
		t.Fatal("one prepared image hashed twice")
	}
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}

	sidecars, err := filepath.Glob(filepath.Join(stateHome, "sysc-notify", "images", "*.png"))
	if err != nil {
		t.Fatal(err)
	}
	if len(sidecars) != 1 {
		t.Fatalf("%d sidecars for one shared image, want 1", len(sidecars))
	}

	reopened, err := OpenAt(stateHome, now)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if len(reopened.entries) != 2 {
		t.Fatalf("%d entries after a reopen, want 2", len(reopened.entries))
	}
	if reopened.entries[0].meta.digest != store.entries[0].meta.digest {
		t.Fatal("the shared sidecar changed identity across a reopen")
	}
}

// TestAddPreparedReusesThePreparedDigest pins that hashing happens once:
// the entry carries the very metadata PrepareImage computed, so the commit
// path never hashes the image again.
func TestAddPreparedReusesThePreparedDigest(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 10, 15, 0, 0, time.UTC)
	store, images := imageStore(t, stateHome, now, []uint32{9})

	entry := testEntry(10, now)
	prepared, err := PrepareImage(images[9])
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.AddPrepared(entry, prepared, now); err != nil {
		t.Fatal(err)
	}
	for _, retained := range store.entries {
		if retained.entry.ID != 10 {
			continue
		}
		if retained.meta != prepared.meta {
			t.Fatal("the store hashed the image again instead of reusing the prepared digest")
		}
	}
}

// TestEntriesShareRetainedImagesWithoutCopying them: an Entries call copies
// the entry structs, never the image bytes behind them.
func TestEntriesShareRetainedImagesWithoutCopying(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 10, 15, 0, 0, time.UTC)
	store, _ := imageStore(t, stateHome, now, []uint32{1})

	first := store.Entries()
	second := store.Entries()
	if first[0].Image != second[0].Image {
		t.Fatal("Entries deep-copied the retained image")
	}
	// The returned structs are private to the caller.
	first[0].Summary = "mutated"
	if store.Entries()[0].Summary != "record" {
		t.Fatal("mutating a returned entry changed the store")
	}
}

// TestDocumentBytesMatchesTheEncodedDocument keeps the measured budget honest:
// the size built from per-entry measurements must equal the real document.
func TestDocumentBytesMatchesTheEncodedDocument(t *testing.T) {
	stateHome := t.TempDir()
	now := time.Date(2026, 8, 30, 10, 15, 0, 0, time.UTC)
	store, _ := imageStore(t, stateHome, now, []uint32{1, 2})
	plain := testEntry(3, now)
	if _, _, err := store.Add(plain, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkSeen([]uint32{1}); err != nil {
		t.Fatal(err)
	}
	entries := cloneRetained(store.entries)
	contents, err := json.Marshal(encodeDocument(entries))
	if err != nil {
		t.Fatal(err)
	}
	if got := documentBytes(entries); got != len(contents)+1 {
		t.Fatalf("documentBytes = %d, encoded document is %d bytes plus the newline", got, len(contents))
	}
}
