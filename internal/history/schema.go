package history

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

const schemaVersion = 1

type document struct {
	Version int         `json:"version"`
	Entries []diskEntry `json:"entries"`
}

type diskEntry struct {
	ID           uint32     `json:"id"`
	Seen         bool       `json:"seen"`
	AppName      string     `json:"app_name,omitempty"`
	AppIcon      string     `json:"app_icon,omitempty"`
	DesktopEntry string     `json:"desktop_entry,omitempty"`
	Summary      string     `json:"summary"`
	Body         string     `json:"body,omitempty"`
	Urgency      uint8      `json:"urgency"`
	Category     string     `json:"category,omitempty"`
	Timestamp    time.Time  `json:"timestamp"`
	Image        *diskImage `json:"image,omitempty"`
}

type diskImage struct {
	SHA256 string `json:"sha256"`
	Width  uint32 `json:"width"`
	Height uint32 `json:"height"`
}

// encodeDocument builds the on-disk document. The image references come from
// the digests the store already knows, so no image is hashed here.
func encodeDocument(entries []retained) document {
	doc := document{Version: schemaVersion, Entries: make([]diskEntry, len(entries))}
	for i := range entries {
		doc.Entries[i] = diskEntryOf(entries[i])
	}
	return doc
}

// diskEntryOf builds the on-disk form of one retained entry.
func diskEntryOf(entry retained) diskEntry {
	return diskEntry{
		ID: entry.entry.ID, Seen: entry.entry.Seen, AppName: entry.entry.AppName, AppIcon: entry.entry.AppIcon,
		DesktopEntry: entry.entry.DesktopEntry, Summary: entry.entry.Summary, Body: entry.entry.Body,
		Urgency: uint8(entry.entry.Urgency), Category: entry.entry.Category, Timestamp: entry.entry.Timestamp.UTC(),
		Image: imageReference(entry.meta),
	}
}

func decodeDocument(doc document, imageDir string) ([]retained, error) {
	if doc.Version != schemaVersion {
		return nil, fmt.Errorf("history: unsupported schema version %d", doc.Version)
	}
	if len(doc.Entries) > protocol.MaxHistoryEntries {
		return nil, errors.New("history: too many entries")
	}
	entries := make([]retained, len(doc.Entries))
	ids := make(map[uint32]struct{}, len(doc.Entries))
	for i, entry := range doc.Entries {
		image, meta, err := loadImage(imageDir, entry.Image)
		if err != nil {
			// ponytail: a broken sidecar costs only that entry's image;
			// quarantine is reserved for structural damage to history.json.
			// cleanupImages then removes the unreferenced sidecar.
			log.Printf("history: drop image for entry %d: %v", entry.ID, err)
			image, meta = nil, nil
		}
		history := protocol.HistoryEntry{
			ID: entry.ID, Seen: entry.Seen, AppName: entry.AppName, AppIcon: entry.AppIcon,
			DesktopEntry: entry.DesktopEntry, Summary: entry.Summary, Body: entry.Body,
			Urgency: protocol.Urgency(entry.Urgency), Category: entry.Category, Timestamp: entry.Timestamp.UTC(), Image: image,
		}
		if err := history.Validate(); err != nil {
			return nil, fmt.Errorf("history: entry %d: %w", i, err)
		}
		if _, exists := ids[entry.ID]; exists {
			return nil, fmt.Errorf("history: duplicate ID %d", entry.ID)
		}
		ids[entry.ID] = struct{}{}
		retained, err := newRetained(history, meta)
		if err != nil {
			return nil, err
		}
		entries[i] = retained
	}
	return entries, nil
}
