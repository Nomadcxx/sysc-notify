package history

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

const (
	historyFilename     = "history.json"
	maxHistoryJSONBytes = 8 << 20
	// maxHistoryWriteBytes leaves headroom below the read limit for the
	// document to grow by a few bytes (seen flags) after the last Add, so the
	// daemon never writes a file its own read path would reject.
	maxHistoryWriteBytes = maxHistoryJSONBytes - 256
)

// errFileTooLarge marks a committed file that exceeds the read limit. Unlike
// other read failures it is recoverable: the file is quarantined and the
// daemon starts with empty history.
var errFileTooLarge = errors.New("history: file exceeds size limit")

// Store keeps the retained history in memory as the authoritative state and
// persists it on a background goroutine. Mutations (Add, Sweep, Remove,
// MarkSeen, Clear) update the in-memory entries synchronously so snapshots
// and deltas never wait on disk, and schedule a single coalesced commit.
// Durability is therefore eventual: call Flush to block until the scheduled
// state is on disk, and Close before exiting the process.
type Store struct {
	dir      string
	imageDir string

	mu        sync.Mutex
	cond      *sync.Cond
	entries   []protocol.HistoryEntry // authoritative in-memory state
	dirty     bool
	writeGen  int // incremented by every mutation
	workerGen int // generation durably committed (or attempted)
	closed    bool
	lastErr   error
	closeOnce sync.Once

	wake       chan struct{}
	closing    chan struct{}
	workerDone chan struct{}
	closeDone  chan struct{}
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
	store := &Store{
		dir: dir, imageDir: imageDir,
		wake: make(chan struct{}, 1), closing: make(chan struct{}), workerDone: make(chan struct{}), closeDone: make(chan struct{}),
	}
	store.cond = sync.NewCond(&store.mu)
	contents, err := readPrivateRegularFile(filepath.Join(dir, historyFilename), maxHistoryJSONBytes)
	if errors.Is(err, os.ErrNotExist) {
		go store.persistLoop()
		return store, nil
	}
	if err != nil && !errors.Is(err, errFileTooLarge) {
		return nil, fmt.Errorf("history: read: %w", err)
	}
	var doc document
	if err == nil {
		decoder := json.NewDecoder(bytes.NewReader(contents))
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&doc); err == nil {
			if err = requireEOF(decoder); err == nil {
				store.entries, err = decodeDocument(doc, imageDir)
			}
		}
	}
	if err != nil {
		if quarantineErr := quarantine(dir, now); quarantineErr != nil {
			return nil, errors.Join(fmt.Errorf("history: invalid committed file: %w", err), quarantineErr)
		}
		go store.persistLoop()
		return store, nil
	}
	retained := retain(store.entries, now)
	if len(retained) != len(store.entries) {
		store.entries = cloneEntries(retained)
		if err := store.commit(retained); err != nil {
			return nil, err
		}
	} else if err := cleanupImages(imageDir, store.entries); err != nil {
		return nil, err
	}
	go store.persistLoop()
	return store, nil
}

// persistLoop owns all disk commits, coalescing concurrent mutations into one
// commit of the latest state. wake carries a stale token (dirty is the
// authoritative flag); closing asks the loop to stop without touching disk.
func (s *Store) persistLoop() {
	defer close(s.workerDone)
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		if s.dirty {
			// Snapshot the authoritative state here, on the worker, rather than
			// on every mutation: the entries are replaced wholesale (never
			// mutated in place), so reading them under the lock is enough, and
			// coalesced bursts clone once instead of once per mutation.
			target := cloneEntries(s.entries)
			gen := s.writeGen
			s.dirty = false
			s.mu.Unlock()
			if err := s.commit(target); err != nil {
				s.recordErr(err)
			} else {
				s.clearErr()
			}
			s.mu.Lock()
			if gen > s.workerGen {
				s.workerGen = gen
			}
			s.cond.Broadcast()
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()
		select {
		case <-s.wake:
		case <-s.closing:
		}
	}
}

func (s *Store) recordErr(err error) {
	log.Printf("sysc-notify: history: persist: %v", err)
	s.mu.Lock()
	if s.lastErr == nil {
		s.lastErr = err
	}
	s.mu.Unlock()
}

// clearErr drops a remembered error once a later commit succeeds, so a
// transient failure during a burst does not make every later Flush and Close
// report an error that has since been resolved.
func (s *Store) clearErr() {
	s.mu.Lock()
	s.lastErr = nil
	s.mu.Unlock()
}

// schedule marks the store dirty and wakes the persistence worker. The worker
// snapshots the entries when it wakes, so no per-mutation copy is needed.
// Callers hold s.mu.
func (s *Store) schedule() {
	s.writeGen++
	s.dirty = true
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Flush blocks until every mutation scheduled before the call is durably
// committed (or has failed). It reports the first persistence error.
func (s *Store) Flush() error {
	s.mu.Lock()
	for s.workerGen < s.writeGen && !s.closed {
		s.cond.Wait()
	}
	if s.closed {
		s.mu.Unlock()
		<-s.closeDone
		s.mu.Lock()
	}
	err := s.lastErr
	s.mu.Unlock()
	return err
}

// Close stops the persistence goroutine and commits any state left pending,
// then reports the first persistence error. The store must not be used after
// Close.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		dirty := s.dirty
		var target []protocol.HistoryEntry
		if dirty {
			target = cloneEntries(s.entries)
		}
		s.mu.Unlock()
		close(s.closing)
		<-s.workerDone
		if dirty {
			if err := s.commit(target); err != nil {
				s.recordErr(err)
			} else {
				s.clearErr()
			}
		}
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
		close(s.closeDone)
	})
	s.mu.Lock()
	err := s.lastErr
	s.mu.Unlock()
	return err
}

func (s *Store) Entries() []protocol.HistoryEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneEntries(s.entries)
}

// IDs returns the ids of the retained entries, oldest first.
func (s *Store) IDs() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]uint32, len(s.entries))
	for i, entry := range s.entries {
		ids[i] = entry.ID
	}
	return ids
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
	s.mu.Lock()
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
	next, budgetRemoved := trimToBudget(next)
	removed = append(removed, budgetRemoved...)
	s.entries = next
	s.schedule()
	s.mu.Unlock()
	return cloneEntry(entry), removed, nil
}

// trimToBudget drops the oldest entries until the encoded document fits the
// write budget. Without it a history of large escaped bodies could exceed the
// read limit and be quarantined on the next start.
func trimToBudget(entries []protocol.HistoryEntry) ([]protocol.HistoryEntry, []uint32) {
	var removed []uint32
	for len(entries) > 0 {
		contents, err := json.Marshal(encodeDocument(entries))
		if err != nil || len(contents)+1 <= maxHistoryWriteBytes {
			return entries, removed
		}
		drop := (len(contents) + 1 - maxHistoryWriteBytes) / (len(contents) / len(entries))
		if drop < 1 {
			drop = 1
		}
		if drop > len(entries) {
			drop = len(entries)
		}
		for _, entry := range entries[:drop] {
			removed = append(removed, entry.ID)
		}
		entries = entries[drop:]
	}
	return entries, removed
}

func (s *Store) Sweep(now time.Time) ([]uint32, error) {
	cutoff := now.Add(-protocol.HistoryRetention)
	s.mu.Lock()
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
		s.mu.Unlock()
		return nil, nil
	}
	s.entries = next
	s.schedule()
	s.mu.Unlock()
	return removed, nil
}

// Remove drops the named entries and returns the ids that were actually
// present. An unknown id is skipped rather than refused: two shells may remove
// the same entry, and the loser must not see a failure for work already done.
func (s *Store) Remove(ids []uint32) ([]uint32, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	drop := make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		drop[id] = struct{}{}
	}
	s.mu.Lock()
	next := make([]protocol.HistoryEntry, 0, len(s.entries))
	removed := make([]uint32, 0, len(ids))
	for _, e := range s.entries {
		if _, ok := drop[e.ID]; ok {
			removed = append(removed, e.ID)
			continue
		}
		next = append(next, cloneEntry(e))
	}
	if len(removed) == 0 {
		s.mu.Unlock()
		return nil, nil
	}
	s.entries = next
	s.schedule()
	s.mu.Unlock()
	return removed, nil
}

func (s *Store) MarkSeen(ids []uint32) ([]uint32, error) {
	wanted := make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	s.mu.Lock()
	next := cloneEntries(s.entries)
	changed := make([]uint32, 0, len(ids))
	for i := range next {
		if _, exists := wanted[next[i].ID]; exists && !next[i].Seen {
			next[i].Seen = true
			changed = append(changed, next[i].ID)
		}
	}
	if len(changed) == 0 {
		s.mu.Unlock()
		return nil, nil
	}
	s.entries = next
	s.schedule()
	s.mu.Unlock()
	return changed, nil
}

func (s *Store) Clear() ([]uint32, error) {
	s.mu.Lock()
	if len(s.entries) == 0 {
		s.mu.Unlock()
		return nil, nil
	}
	removed := make([]uint32, len(s.entries))
	for i, entry := range s.entries {
		removed[i] = entry.ID
	}
	s.entries = nil
	s.schedule()
	s.mu.Unlock()
	return removed, nil
}

// commit writes the given entries atomically (temporary file, fsync, rename,
// directory sync) and cleans up orphaned image sidecars. It is called only by
// the persistence loop or during OpenAt; it never touches s.entries.
func (s *Store) commit(entries []protocol.HistoryEntry) error {
	for i := range entries {
		if entries[i].Image == nil {
			continue
		}
		if err := writeImage(s.imageDir, entries[i].Image); err != nil {
			return fmt.Errorf("history: write image: %w", err)
		}
	}
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
	if err := syncDirectory(s.dir); err != nil {
		return err
	}
	if err := cleanupImages(s.imageDir, entries); err != nil {
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
		return nil, fmt.Errorf("%w: %q exceeds %d bytes", errFileTooLarge, path, limit)
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
