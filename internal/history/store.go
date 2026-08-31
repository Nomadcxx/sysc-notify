package history

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

const (
	historyFilename     = "history.json"
	maxHistoryJSONBytes = 8 << 20
)

type Store struct {
	dir      string
	imageDir string
	entries  []protocol.HistoryEntry
}

func Open(now time.Time) (*Store, error) {
	stateHome := os.Getenv("XDG_STATE_HOME")
	if stateHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("history: resolve home: %w", err)
		}
		stateHome = filepath.Join(home, ".local", "state")
	}
	return OpenAt(stateHome, now)
}

func OpenAt(stateHome string, now time.Time) (*Store, error) {
	abs, err := filepath.Abs(stateHome)
	if err != nil {
		return nil, fmt.Errorf("history: resolve state path: %w", err)
	}
	if err := makePath(abs); err != nil {
		return nil, err
	}
	dir := filepath.Join(abs, "sysc-notify")
	if err := makePath(dir); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("history: secure state directory: %w", err)
	}
	imageDir := filepath.Join(dir, "images")
	if err := makePath(imageDir); err != nil {
		return nil, err
	}
	if err := os.Chmod(imageDir, 0o700); err != nil {
		return nil, fmt.Errorf("history: secure image directory: %w", err)
	}
	store := &Store{dir: dir, imageDir: imageDir}
	contents, err := readPrivateRegularFile(filepath.Join(dir, historyFilename), maxHistoryJSONBytes)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("history: read: %w", err)
	}
	var doc document
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&doc); err == nil {
		if err = requireEOF(decoder); err == nil {
			store.entries, err = decodeDocument(doc, imageDir)
		}
	}
	if err != nil {
		if quarantineErr := quarantine(dir, now); quarantineErr != nil {
			return nil, errors.Join(fmt.Errorf("history: invalid committed file: %w", err), quarantineErr)
		}
		return store, nil
	}
	retained := retain(store.entries, now)
	if len(retained) != len(store.entries) {
		if err := store.commit(retained); err != nil {
			return nil, err
		}
	} else if err := cleanupImages(imageDir, store.entries); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) Entries() []protocol.HistoryEntry {
	return cloneEntries(s.entries)
}

func (s *Store) Add(entry protocol.HistoryEntry, now time.Time) (protocol.HistoryEntry, []uint32, error) {
	entry.Timestamp = entry.Timestamp.UTC()
	image, err := normalizeHistoryImage(entry.Image)
	if err != nil {
		return protocol.HistoryEntry{}, nil, err
	}
	entry.Image = image
	if err := entry.Validate(); err != nil {
		return protocol.HistoryEntry{}, nil, err
	}
	if entry.Timestamp.Before(now.Add(-protocol.HistoryRetention)) {
		return protocol.HistoryEntry{}, nil, nil
	}
	next := cloneEntries(s.entries)
	removed := make([]uint32, 0, 2)
	for i := 0; i < len(next); {
		if next[i].ID == entry.ID || next[i].Timestamp.Before(now.Add(-protocol.HistoryRetention)) {
			removed = append(removed, next[i].ID)
			next = append(next[:i], next[i+1:]...)
			continue
		}
		i++
	}
	next = append(next, entry)
	for len(next) > protocol.MaxHistoryEntries {
		removed = append(removed, next[0].ID)
		next = next[1:]
	}
	if err := writeImage(s.imageDir, entry.Image); err != nil {
		return protocol.HistoryEntry{}, nil, err
	}
	if err := s.commit(next); err != nil {
		return protocol.HistoryEntry{}, nil, err
	}
	return cloneEntry(entry), removed, nil
}

func (s *Store) Sweep(now time.Time) ([]uint32, error) {
	cutoff := now.Add(-protocol.HistoryRetention)
	next := make([]protocol.HistoryEntry, 0, len(s.entries))
	removed := make([]uint32, 0)
	for _, entry := range s.entries {
		if entry.Timestamp.Before(cutoff) {
			removed = append(removed, entry.ID)
			continue
		}
		next = append(next, cloneEntry(entry))
	}
	if len(removed) == 0 {
		return nil, nil
	}
	if err := s.commit(next); err != nil {
		return nil, err
	}
	return removed, nil
}

func (s *Store) MarkSeen(ids []uint32) ([]uint32, error) {
	wanted := make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	next := cloneEntries(s.entries)
	changed := make([]uint32, 0, len(ids))
	for i := range next {
		if _, exists := wanted[next[i].ID]; exists && !next[i].Seen {
			next[i].Seen = true
			changed = append(changed, next[i].ID)
		}
	}
	if len(changed) == 0 {
		return nil, nil
	}
	if err := s.commit(next); err != nil {
		return nil, err
	}
	return changed, nil
}

func (s *Store) Clear() ([]uint32, error) {
	if len(s.entries) == 0 {
		return nil, nil
	}
	removed := make([]uint32, len(s.entries))
	for i, entry := range s.entries {
		removed[i] = entry.ID
	}
	if err := s.commit(nil); err != nil {
		return nil, err
	}
	return removed, nil
}

func (s *Store) commit(entries []protocol.HistoryEntry) error {
	contents, err := json.Marshal(encodeDocument(entries))
	if err != nil {
		return fmt.Errorf("history: encode: %w", err)
	}
	contents = append(contents, '\n')
	temporary, err := os.CreateTemp(s.dir, ".history.json.tmp-")
	if err != nil {
		return fmt.Errorf("history: create temporary file: %w", err)
	}
	temporaryName := temporary.Name()
	keep := false
	defer func() {
		_ = temporary.Close()
		if !keep {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("history: secure temporary file: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		return fmt.Errorf("history: write temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("history: sync temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("history: close temporary file: %w", err)
	}
	if err := os.Rename(temporaryName, filepath.Join(s.dir, historyFilename)); err != nil {
		return fmt.Errorf("history: commit: %w", err)
	}
	keep = true
	s.entries = cloneEntries(entries)
	if err := syncDirectory(s.dir); err != nil {
		return err
	}
	if err := cleanupImages(s.imageDir, s.entries); err != nil {
		return err
	}
	return nil
}

func readPrivateRegularFile(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, fmt.Errorf("history: unsafe file %q", path)
	}
	if info.Size() > limit {
		return nil, fmt.Errorf("history: file %q exceeds %d bytes", path, limit)
	}
	return os.ReadFile(path)
}

func makePath(path string) error {
	clean := filepath.Clean(path)
	current := string(filepath.Separator)
	for _, component := range splitPath(clean) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
				return fmt.Errorf("history: create %q: %w", current, err)
			}
			info, err = os.Lstat(current)
		}
		if err != nil {
			return fmt.Errorf("history: inspect %q: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("history: unsafe path component %q", current)
		}
	}
	return nil
}

func splitPath(path string) []string {
	volume := filepath.VolumeName(path)
	path = path[len(volume):]
	path = bytes.NewBufferString(path).String()
	var parts []string
	for path != string(filepath.Separator) && path != "." && path != "" {
		dir, base := filepath.Split(path)
		if base != "" {
			parts = append([]string{base}, parts...)
		}
		path = filepath.Clean(dir)
	}
	return parts
}

func quarantine(dir string, now time.Time) error {
	random := make([]byte, 6)
	if _, err := rand.Read(random); err != nil {
		return fmt.Errorf("history: generate quarantine name: %w", err)
	}
	name := fmt.Sprintf("history.quarantine-%s-%s.json", now.UTC().Format("20060102T150405.000000000Z"), hex.EncodeToString(random))
	if err := os.Rename(filepath.Join(dir, historyFilename), filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("history: quarantine invalid file: %w", err)
	}
	return syncDirectory(dir)
}

func requireEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("history: trailing JSON value")
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("history: open directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("history: sync directory: %w", err)
	}
	return nil
}

func retain(entries []protocol.HistoryEntry, now time.Time) []protocol.HistoryEntry {
	cutoff := now.Add(-protocol.HistoryRetention)
	kept := make([]protocol.HistoryEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.Timestamp.Before(cutoff) {
			kept = append(kept, cloneEntry(entry))
		}
	}
	return kept
}

func cloneEntries(entries []protocol.HistoryEntry) []protocol.HistoryEntry {
	cloned := make([]protocol.HistoryEntry, len(entries))
	for i, entry := range entries {
		cloned[i] = cloneEntry(entry)
	}
	return cloned
}

func cloneEntry(entry protocol.HistoryEntry) protocol.HistoryEntry {
	entry.Image = cloneImage(entry.Image)
	return entry
}

func cloneImage(image *protocol.Image) *protocol.Image {
	if image == nil {
		return nil
	}
	cloned := *image
	cloned.Data = append([]byte(nil), image.Data...)
	return &cloned
}
