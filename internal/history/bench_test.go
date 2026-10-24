package history

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"strings"
	"testing"
	"time"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

// benchNow is the clock every history benchmark shares, so the fixtures are
// reproducible.
var benchNow = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

// noisyPNG builds a deterministic random image so every entry names its own
// sidecar, the way a burst of rich notifications does. Random pixels defeat
// the PNG compressor, so the sidecars are their real size.
func noisyPNG(tb testing.TB, seed uint64) *protocol.Image {
	tb.Helper()
	const edge = historyImageLongEdge
	img := image.NewNRGBA(image.Rect(0, 0, edge, edge))
	state := seed*6364136223846793005 + 1442695040888963407
	for y := 0; y < edge; y++ {
		for x := 0; x < edge; x++ {
			state = state*6364136223846793005 + 1442695040888963407
			v := byte(state >> 33)
			img.SetNRGBA(x, y, color.NRGBA{R: v, G: uint8(state >> 25), B: uint8(state >> 17), A: 0xff})
		}
	}
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, img); err != nil {
		tb.Fatal(err)
	}
	return &protocol.Image{MediaType: "image/png", Width: edge, Height: edge, Data: encoded.Bytes()}
}

// benchEntries builds a full history: every entry carries a ~300-byte body
// and, when images is set, its own image.
func benchEntries(tb testing.TB, count int, images bool) []protocol.HistoryEntry {
	tb.Helper()
	entries := make([]protocol.HistoryEntry, 0, count)
	for id := 1; id <= count; id++ {
		entry := protocol.HistoryEntry{
			ID:      uint32(id),
			AppName: "Firefox",
			Summary: "Download finished",
			Body:    strings.Repeat("payload ", 38),
			Urgency: protocol.UrgencyNormal,
			// The store keeps entries oldest first; the timestamps advance
			// so retention never drops the fixture under the benchmark.
			Timestamp: benchNow.Add(-time.Duration(count-id) * time.Second),
		}
		if images {
			entry.Image = noisyPNG(tb, uint64(id))
		}
		entries = append(entries, entry)
	}
	return entries
}

// benchStore holds a full history whose sidecars and history.json are already
// on disk, the way a daemon that has been up for a while does.
func benchStore(b *testing.B, entries []protocol.HistoryEntry) *Store {
	b.Helper()
	store, err := OpenAt(b.TempDir(), benchNow)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	for _, entry := range entries {
		prepared, err := PrepareImage(entry.Image)
		if err != nil {
			b.Fatal(err)
		}
		if _, _, err := store.AddPrepared(entry, prepared, benchNow); err != nil {
			b.Fatal(err)
		}
	}
	if err := store.Flush(); err != nil {
		b.Fatal(err)
	}
	return store
}

// BenchmarkCommitFull100Images measures the commit that closes a burst of
// rich notifications: a full image history whose sidecars are all already
// on disk. The steady-state commit must stat those sidecars and never read,
// hash or decode them (issue #61).
func BenchmarkCommitFull100Images(b *testing.B) {
	b.ReportAllocs()
	store := benchStore(b, benchEntries(b, protocol.MaxHistoryEntries, true))
	target := cloneRetained(store.entries)
	for b.Loop() {
		if err := store.commit(target); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCommitFull100Text measures the same commit with no images, the
// floor the image path must stay close to.
func BenchmarkCommitFull100Text(b *testing.B) {
	b.ReportAllocs()
	store := benchStore(b, benchEntries(b, protocol.MaxHistoryEntries, false))
	target := cloneRetained(store.entries)
	for b.Loop() {
		if err := store.commit(target); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAddPreparedFull100Images measures one notification added to a
// full image history. The persistence worker is stopped (Close commits the
// fixture and returns) so the loop measures the mutation path alone: the
// caller must not copy the retained images or re-measure the document.
func BenchmarkAddPreparedFull100Images(b *testing.B) {
	b.ReportAllocs()
	entries := benchEntries(b, protocol.MaxHistoryEntries, true)
	store := benchStore(b, entries)
	if err := store.Close(); err != nil {
		b.Fatal(err)
	}
	prepared, err := PrepareImage(entries[0].Image)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; b.Loop(); i++ {
		entry := entries[0]
		entry.ID = uint32(protocol.MaxHistoryEntries + i + 1)
		if _, _, err := store.AddPrepared(entry, prepared, benchNow); err != nil {
			b.Fatal(err)
		}
	}
}
