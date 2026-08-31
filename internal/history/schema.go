package history

import (
	"errors"
	"fmt"
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

func encodeDocument(entries []protocol.HistoryEntry) document {
	doc := document{Version: schemaVersion, Entries: make([]diskEntry, len(entries))}
	for i, entry := range entries {
		doc.Entries[i] = diskEntry{
			ID: entry.ID, Seen: entry.Seen, AppName: entry.AppName, AppIcon: entry.AppIcon,
			DesktopEntry: entry.DesktopEntry, Summary: entry.Summary, Body: entry.Body,
			Urgency: uint8(entry.Urgency), Category: entry.Category, Timestamp: entry.Timestamp.UTC(),
			Image: imageReference(entry.Image),
		}
	}
	return doc
}

func decodeDocument(doc document, imageDir string) ([]protocol.HistoryEntry, error) {
	if doc.Version != schemaVersion {
		return nil, fmt.Errorf("history: unsupported schema version %d", doc.Version)
	}
	if len(doc.Entries) > protocol.MaxHistoryEntries {
		return nil, errors.New("history: too many entries")
	}
	entries := make([]protocol.HistoryEntry, len(doc.Entries))
	ids := make(map[uint32]struct{}, len(doc.Entries))
	for i, entry := range doc.Entries {
		image, err := loadImage(imageDir, entry.Image)
		if err != nil {
			return nil, fmt.Errorf("history: entry %d image: %w", i, err)
		}
		entries[i] = protocol.HistoryEntry{
			ID: entry.ID, Seen: entry.Seen, AppName: entry.AppName, AppIcon: entry.AppIcon,
			DesktopEntry: entry.DesktopEntry, Summary: entry.Summary, Body: entry.Body,
			Urgency: protocol.Urgency(entry.Urgency), Category: entry.Category, Timestamp: entry.Timestamp.UTC(), Image: image,
		}
		if err := entries[i].Validate(); err != nil {
			return nil, fmt.Errorf("history: entry %d: %w", i, err)
		}
		if _, exists := ids[entry.ID]; exists {
			return nil, fmt.Errorf("history: duplicate ID %d", entry.ID)
		}
		ids[entry.ID] = struct{}{}
	}
	return entries, nil
}
