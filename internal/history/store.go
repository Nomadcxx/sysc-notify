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
	// persistRetryDelay is how long the worker waits before it tries again
	// after a failed commit, so a disk that stays full does not spin it.
	persistRetryDelay = 250 * time.Millisecond
)

// errFileTooLarge marks a committed file that exceeds the read limit. Unlike
// other read failures it is recoverable: the file is quarantined and the
// daemon starts with empty history.
var errFileTooLarge = errors.New("history: file exceeds size limit")

// retained is one history entry with what the store remembers about it: the
// metadata of its image and the size of its encoded form. Both are derived
// from data that never changes, so they are computed once and carried along
// instead of being hashed and measured again on every mutation.
type retained struct {
	entry protocol.HistoryEntry
	meta  *imageMeta
	size  int
}

// newRetained pairs an entry with its image metadata and measures the entry
// as it is written to history.json.
func newRetained(entry protocol.HistoryEntry, meta *imageMeta) (retained, error) {
	encoded, err := json.Marshal(diskEntryOf(retained{entry: entry, meta: meta}))
	if err != nil {
		return retained{}, fmt.Errorf("history: measure entry %d: %w", entry.ID, err)
	}
	return retained{entry: entry, meta: meta, size: len(encoded)}, nil
}

// cloneRetained copies the entry list. The images are immutable once
// prepared, so the copy shares them instead of copying their bytes.
func cloneRetained(entries []retained) []retained {
	cloned := make([]retained, len(entries))
	copy(cloned, entries)
	return cloned
}

func cloneEntry(entry protocol.HistoryEntry) protocol.HistoryEntry {
	if entry.Image != nil {
		image := *entry.Image
		image.Data = bytes.Clone(image.Data)
		entry.Image = &image
	}
	return entry
}

// documentOverhead is the size of the encoded document around its entries:
// the version, the array and the trailing newline.
var documentOverhead = func() int {
	empty, err := json.Marshal(document{Version: schemaVersion, Entries: []diskEntry{}})
	if err != nil {
		return 0
	}
	return len(empty) + 1
}()

// documentBytes is the exact size of the encoded document for the entries.
// The entry sizes are measured when the entries are added, so the write
// budget is checked without marshalling the document.
func documentBytes(entries []retained) int {
	if len(entries) == 0 {
		return documentOverhead
	}
	// The empty array of the overhead gains the entries between its brackets,
	// and every entry but the first adds a comma.
	total := documentOverhead + len(entries) - 1
	for i := range entries {
		total += entries[i].size
	}
	return total
}

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
	entries   []retained // authoritative in-memory state
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
		name, quarantineErr := quarantine(dir, now, syncDirectory)
		if quarantineErr != nil {
			return nil, errors.Join(fmt.Errorf("history: invalid committed file: %w", err), quarantineErr)
		}
		// The committed file is gone and the notification centre starts empty,
		// so say so. Without this the history just looks like it was never
		// recorded and nothing points at the file that still has the entries.
		log.Printf("sysc-notify: history: committed file was unusable (%v), moved to %s, starting with empty history", err, name)
		go store.persistLoop()
		return store, nil
	}
	retained := retain(store.entries, now)
	if len(retained) != len(store.entries) {
		store.entries = retained
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
			target := cloneRetained(s.entries)
			gen := s.writeGen
			s.dirty = false
			s.mu.Unlock()
			err := s.commit(target)
			if err != nil {
				s.recordErr(err)
			} else {
				s.clearErr()
			}
			s.mu.Lock()
			if err != nil {
				// The snapshot did not reach the disk. Keep the store dirty
				// so that the next attempt and Close both write it; a failed
				// attempt is not a durable one.
				s.dirty = true
			}
			if gen > s.workerGen {
				s.workerGen = gen
			}
			s.cond.Broadcast()
			s.mu.Unlock()
			if err != nil {
				// Wait before the next attempt, so a disk that stays full does
				// not spin the worker.
				select {
				case <-s.closing:
				case <-time.After(persistRetryDelay):
				}
			}
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
		s.mu.Unlock()
		close(s.closing)
		<-s.workerDone
		// Read what is pending once the worker has stopped: a commit that was
		// in flight when the close began may have failed, and that failure
		// leaves the snapshot dirty for this last write.
		s.mu.Lock()
		dirty := s.dirty
		var target []retained
		if dirty {
			target = cloneRetained(s.entries)
		}
		s.mu.Unlock()
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

// Entries returns copies of the retained entries and their image data.
func (s *Store) Entries() []protocol.HistoryEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]protocol.HistoryEntry, len(s.entries))
	for i := range s.entries {
		entries[i] = cloneEntry(s.entries[i].entry)
	}
	return entries
}

// IDs returns the ids of the retained entries, oldest first.
func (s *Store) IDs() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]uint32, len(s.entries))
	for i, entry := range s.entries {
		ids[i] = entry.entry.ID
	}
	return ids
}

func (s *Store) Add(entry protocol.HistoryEntry, now time.Time) (protocol.HistoryEntry, []uint32, error) {
	image, err := PrepareImage(entry.Image)
	if err != nil {
		return protocol.HistoryEntry{}, nil, err
	}
	return s.AddPrepared(entry, image, now)
}

// AddPrepared stores an image normalized before the caller entered a serialized
// state owner. PrepareImage is the only constructor for a non-empty value.
func (s *Store) AddPrepared(entry protocol.HistoryEntry, image PreparedImage, now time.Time) (protocol.HistoryEntry, []uint32, error) {
	entry.Timestamp = entry.Timestamp.UTC()
	entry.Image = image.image
	if err := entry.Validate(); err != nil {
		return protocol.HistoryEntry{}, nil, err
	}
	if entry.Timestamp.Before(now.Add(-protocol.HistoryRetention)) {
		return protocol.HistoryEntry{}, nil, nil
	}
	s.mu.Lock()
	next := cloneRetained(s.entries)
	removed := make([]uint32, 0, 2)
	for i := 0; i < len(next); {
		if next[i].entry.ID == entry.ID || next[i].entry.Timestamp.Before(now.Add(-protocol.HistoryRetention)) {
			removed = append(removed, next[i].entry.ID)
			next = append(next[:i], next[i+1:]...)
			continue
		}
		i++
	}
	added, err := newRetained(entry, image.meta)
	if err != nil {
		s.mu.Unlock()
		return protocol.HistoryEntry{}, nil, err
	}
	next = append(next, added)
	for len(next) > protocol.MaxHistoryEntries {
		removed = append(removed, next[0].entry.ID)
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
// read limit and be quarantined on the next start. The entry sizes are known,
// so the document is not marshalled to measure it.
func trimToBudget(entries []retained) ([]retained, []uint32) {
	var removed []uint32
	for len(entries) > 0 {
		if documentBytes(entries) <= maxHistoryWriteBytes {
			return entries, removed
		}
		removed = append(removed, entries[0].entry.ID)
		entries = entries[1:]
	}
	return entries, removed
}

func (s *Store) Sweep(now time.Time) ([]uint32, error) {
	cutoff := now.Add(-protocol.HistoryRetention)
	s.mu.Lock()
	next := make([]retained, 0, len(s.entries))
	removed := make([]uint32, 0)
	for _, entry := range s.entries {
		if entry.entry.Timestamp.Before(cutoff) {
			removed = append(removed, entry.entry.ID)
			continue
		}
		next = append(next, entry)
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
	next := make([]retained, 0, len(s.entries))
	removed := make([]uint32, 0, len(ids))
	for _, e := range s.entries {
		if _, ok := drop[e.entry.ID]; ok {
			removed = append(removed, e.entry.ID)
			continue
		}
		next = append(next, e)
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
	next := cloneRetained(s.entries)
	changed := make([]uint32, 0, len(ids))
	for i := range next {
		if _, exists := wanted[next[i].entry.ID]; exists && !next[i].entry.Seen {
			next[i].entry.Seen = true
			// The seen flag changes the encoded size of the entry, so the
			// entry is measured again.
			measured, err := newRetained(next[i].entry, next[i].meta)
			if err != nil {
				s.mu.Unlock()
				return nil, err
			}
			next[i] = measured
			changed = append(changed, next[i].entry.ID)
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
		removed[i] = entry.entry.ID
	}
	s.entries = nil
	s.schedule()
	s.mu.Unlock()
	return removed, nil
}

// commit writes the given entries atomically (temporary file, fsync, rename,
// directory sync) and cleans up orphaned image sidecars. It is called only by
// the persistence loop or during OpenAt; it never touches s.entries.
func (s *Store) commit(entries []retained) error {
	var sidecarBuffer []byte
	for i := range entries {
		if entries[i].meta == nil {
			continue
		}
		if sidecarBuffer == nil {
			sidecarBuffer = make([]byte, 32*1024)
		}
		if err := writeImage(s.imageDir, entries[i].entry.Image, entries[i].meta, sidecarBuffer); err != nil {
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
	info, err := privateRegularFileInfo(path)
	if err != nil {
		return nil, err
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

// quarantine moves an unusable history.json aside and returns its new name so
// the caller can tell the user where the entries went.
func quarantine(dir string, now time.Time, syncDir func(string) error) (string, error) {
	random := make([]byte, 6)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("history: generate quarantine name: %w", err)
	}
	name := fmt.Sprintf("history.quarantine-%s-%s.json", now.UTC().Format("20060102T150405.000000000Z"), hex.EncodeToString(random))
	if err := os.Rename(filepath.Join(dir, historyFilename), filepath.Join(dir, name)); err != nil {
		return "", fmt.Errorf("history: quarantine invalid file: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return name, fmt.Errorf("history: quarantine moved file to %s: %w", name, err)
	}
	return name, nil
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

func retain(entries []retained, now time.Time) []retained {
	cutoff := now.Add(-protocol.HistoryRetention)
	kept := make([]retained, 0, len(entries))
	for _, entry := range entries {
		if !entry.entry.Timestamp.Before(cutoff) {
			kept = append(kept, entry)
		}
	}
	return kept
}
