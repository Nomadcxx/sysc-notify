package state

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Nomadcxx/sysc-notify/internal/history"
	"github.com/Nomadcxx/sysc-notify/internal/notify"
	"github.com/Nomadcxx/sysc-notify/protocol"
)

const (
	DefaultTimeout    = 5 * time.Second
	PresentationLease = 6 * time.Second
	HistorySweep      = time.Minute
)

var ErrNotFound = errors.New("state: notification not found")

type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

type Sink interface {
	Publish(Event) bool
}

type Event struct {
	Sequence uint64
	Delta    *protocol.Delta
	Action   *ActionEvent
	Reply    *ReplyEvent
	// Snapshot carries a full projection in response to PresenterResync. It is
	// published on the same channel as deltas and serialized with them by the
	// owner goroutine, so its Sequence is a valid baseline that no later delta
	// predates. Generation scopes it to the connection that asked for it.
	Snapshot   *protocol.Snapshot
	Generation uint64
}

type ActionEvent struct {
	ID  uint32
	Key string
}

type ReplyEvent struct {
	ID   uint32
	Text string
}

type CommandKind uint8

const (
	Add CommandKind = iota + 1
	CloseRequested
	Dismiss
	InvokeAction
	SubmitReply
	PresentationRenew
	PresenterLost
	DismissAll
	HistoryClear
	HistoryRemove
	HistoryMarkSeen
	ProducerPublish
	ProducerClose
	// PresenterResync asks for a fresh snapshot on the owner goroutine so a
	// presenter that overflowed its bounded queue can be rebased without a
	// reconnect. It never mutates state and never advances the sequence.
	PresenterResync
)

type Command struct {
	Kind          CommandKind
	Candidate     notify.Candidate
	Producer      *protocol.ProducerRequest
	ID            uint32
	ActionKey     string
	ReplyText     string
	Generation    uint64
	IDs           []uint32
	Presentations []protocol.Presentation
}

type Result struct {
	ID        uint32
	Replaced  bool
	Lifetimes []protocol.Lifetime
}

type Owner struct {
	requests chan request
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
	store    *history.Store
}

type request struct {
	command  *Command
	snapshot bool
	reply    chan response
}

type response struct {
	result   Result
	snapshot protocol.Snapshot
	err      error
}

func Start(clock Clock, sink Sink) *Owner {
	return StartWithHistory(clock, sink, nil)
}

func StartWithHistory(clock Clock, sink Sink, store *history.Store) *Owner {
	if clock == nil {
		clock = systemClock{}
	}
	o := &Owner{
		requests: make(chan request),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		store:    store,
	}
	go o.run(clock, sink, store)
	return o
}

func (o *Owner) Do(ctx context.Context, command Command) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	reply := make(chan response, 1)
	request := request{command: &command, reply: reply}
	select {
	case o.requests <- request:
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-o.done:
		return Result{}, errors.New("state: owner closed")
	}
	select {
	case response := <-reply:
		return response.result, response.err
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-o.done:
		return Result{}, errors.New("state: owner closed")
	}
}

func (o *Owner) Snapshot(ctx context.Context) (protocol.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return protocol.Snapshot{}, err
	}
	reply := make(chan response, 1)
	select {
	case o.requests <- request{snapshot: true, reply: reply}:
	case <-ctx.Done():
		return protocol.Snapshot{}, ctx.Err()
	case <-o.done:
		return protocol.Snapshot{}, errors.New("state: owner closed")
	}
	select {
	case response := <-reply:
		return response.snapshot, response.err
	case <-ctx.Done():
		return protocol.Snapshot{}, ctx.Err()
	case <-o.done:
		return protocol.Snapshot{}, errors.New("state: owner closed")
	}
}

func (o *Owner) Close() error {
	o.once.Do(func() { close(o.stop) })
	<-o.done
	if o.store != nil {
		return o.store.Flush()
	}
	return nil
}

func (o *Owner) run(clock Clock, sink Sink, store *history.Store) {
	defer close(o.done)
	state := ownerState{
		clock: clock, sink: sink, history: store, records: make(map[uint32]*record), nextID: 1,
		producerIDs: make(map[string]uint32),
	}
	if store != nil {
		state.nextHistorySweep = clock.Now().Add(HistorySweep)
		// Continue after the ids history kept, so ids stay unique across restarts.
		for _, id := range store.IDs() {
			state.nextID = max(state.nextID, id+1)
		}
	}
	state.resetTimer()
	for {
		select {
		case request := <-o.requests:
			if request.snapshot {
				request.reply <- response{snapshot: state.snapshot()}
				continue
			}
			result, err := state.do(*request.command)
			state.resetTimer()
			request.reply <- response{result: result, err: err}
		case <-state.timerC:
			state.handleTimer()
			state.resetTimer()
		case <-o.stop:
			if state.timer != nil {
				state.timer.Stop()
			}
			return
		}
	}
}

type record struct {
	notification protocol.Notification
	candidate    notify.Candidate
	producerKey  string
	duration     time.Duration
	state        protocol.PresentationState
	displayed    bool
	remaining    time.Duration
	deadline     time.Time
	hasDeadline  bool
}

type ownerState struct {
	clock   Clock
	sink    Sink
	history *history.Store

	records     map[uint32]*record
	producerIDs map[string]uint32
	order       []uint32
	nextID      uint32
	sequence    uint64

	presenterGeneration uint64
	leaseDeadline       time.Time
	timer               Timer
	timerC              <-chan time.Time
	nextHistorySweep    time.Time
}

func (s *ownerState) do(command Command) (Result, error) {
	now := s.clock.Now()
	switch command.Kind {
	case Add:
		return s.add(command.Candidate, now)
	case ProducerPublish:
		return s.publishProducer(command.Producer, now)
	case ProducerClose:
		return s.closeProducer(command.Producer)
	case CloseRequested:
		return Result{}, s.close(command.ID, protocol.CloseRequested)
	case Dismiss:
		return Result{}, s.close(command.ID, protocol.CloseDismissed)
	case InvokeAction:
		return Result{}, s.invokeAction(command.ID, command.ActionKey)
	case SubmitReply:
		return Result{}, s.submitReply(command.ID, command.ReplyText)
	case PresentationRenew:
		if err := s.renew(command.Generation, command.Presentations, now); err != nil {
			return Result{}, err
		}
		return Result{Lifetimes: s.lifetimes(now)}, nil
	case PresenterLost:
		if command.Generation != 0 && command.Generation == s.presenterGeneration {
			s.clearPresentation(now)
		}
		return Result{}, nil
	case DismissAll:
		var result error
		for _, id := range append([]uint32(nil), s.order...) {
			result = errors.Join(result, s.close(id, protocol.CloseDismissed))
		}
		return Result{}, result
	case HistoryClear:
		return Result{}, s.clearHistory()
	case HistoryRemove:
		return Result{}, s.removeHistory(command.IDs)
	case HistoryMarkSeen:
		return Result{}, s.markHistorySeen(command.IDs)
	case PresenterResync:
		snapshot := s.snapshot()
		s.publish(Event{Generation: command.Generation, Sequence: snapshot.Sequence, Snapshot: &snapshot})
		return Result{}, nil
	default:
		return Result{}, errors.New("state: unknown command")
	}
}

func (s *ownerState) add(candidate notify.Candidate, now time.Time) (Result, error) {
	return s.addWithProducerKey(candidate, "", now)
}

func (s *ownerState) addWithProducerKey(candidate notify.Candidate, producerKey string, now time.Time) (Result, error) {
	if err := validateCandidate(candidate, now); err != nil {
		return Result{}, err
	}
	id := candidate.ReplacesID
	existing, replaced := s.records[id]
	if !replaced {
		if len(s.records) == protocol.MaxActiveNotifications {
			s.evictCapacityVictim()
		}
		id, s.nextID = notify.ResolveID(0, s.nextID, s.takenIDs())
	}

	notification := notificationFromCandidate(id, candidate, now)
	duration := candidateDuration(candidate)
	record := &record{
		notification: notification,
		candidate:    candidate,
		producerKey:  producerKey,
		duration:     duration,
		state:        protocol.PresentationSuppressed,
		remaining:    duration,
	}
	if replaced {
		if existing.producerKey != "" && existing.producerKey != producerKey && s.producerIDs[existing.producerKey] == id {
			delete(s.producerIDs, existing.producerKey)
		}
		record.state = existing.state
		record.displayed = existing.displayed
		record.producerKey = producerKey
		if duration > 0 {
			switch record.state {
			case protocol.PresentationQueued, protocol.PresentationHovered:
			default:
				record.deadline = now.Add(duration)
				record.hasDeadline = true
			}
		}
		s.records[id] = record
		lifetime := s.lifetime(record, now)
		s.publishDelta(protocol.Delta{Kind: protocol.DeltaReplaced, Notification: cloneNotificationPointer(notification), Lifetime: &lifetime})
		return Result{ID: id, Replaced: true}, nil
	}
	if duration > 0 {
		record.deadline = now.Add(duration)
		record.hasDeadline = true
	}
	s.records[id] = record
	s.order = append(s.order, id)
	lifetime := s.lifetime(record, now)
	s.publishDelta(protocol.Delta{Kind: protocol.DeltaAdded, Notification: cloneNotificationPointer(notification), Lifetime: &lifetime})
	return Result{ID: id}, nil
}

func (s *ownerState) publishProducer(request *protocol.ProducerRequest, now time.Time) (Result, error) {
	if request == nil {
		return Result{}, errors.New("state: missing producer request")
	}
	if err := request.Validate(); err != nil {
		return Result{}, err
	}
	if s.producerIDs == nil {
		s.producerIDs = make(map[string]uint32)
	}
	replacesID := s.producerIDs[request.Key]
	if replacesID != 0 {
		if _, exists := s.records[replacesID]; !exists {
			delete(s.producerIDs, request.Key)
			replacesID = 0
		}
	}
	value := cloneInt32(request.Value)
	candidate := notify.Candidate{
		AppName: request.AppName, Summary: request.Summary, Body: request.Body,
		ReplacesID: replacesID, Urgency: request.Urgency, ExpireTimeout: request.ExpireTimeoutMS,
		Transient: true, Value: value,
	}
	result, err := s.addWithProducerKey(candidate, request.Key, now)
	if err != nil {
		return Result{}, err
	}
	s.producerIDs[request.Key] = result.ID
	return result, nil
}

func (s *ownerState) closeProducer(request *protocol.ProducerRequest) (Result, error) {
	if request == nil {
		return Result{}, errors.New("state: missing producer request")
	}
	if err := request.Validate(); err != nil {
		return Result{}, err
	}
	id := s.producerIDs[request.Key]
	if id == 0 {
		return Result{}, nil
	}
	if _, exists := s.records[id]; !exists {
		delete(s.producerIDs, request.Key)
		return Result{}, nil
	}
	if err := s.close(id, protocol.CloseRequested); err != nil {
		return Result{ID: id}, err
	}
	return Result{ID: id}, nil
}

// takenIDs holds the ids a new notification must not use: the live ones and
// the ones closed history still names, since a history entry is replaced by id.
func (s *ownerState) takenIDs() map[uint32]struct{} {
	taken := make(map[uint32]struct{}, len(s.records))
	for id := range s.records {
		taken[id] = struct{}{}
	}
	if s.history != nil {
		for _, id := range s.history.IDs() {
			taken[id] = struct{}{}
		}
	}
	return taken
}

func validateCandidate(candidate notify.Candidate, now time.Time) error {
	if candidate.ExpireTimeout < -1 {
		return errors.New("state: invalid expiry timeout")
	}
	n := notificationFromCandidate(1, candidate, now)
	return n.Validate()
}

func notificationFromCandidate(id uint32, candidate notify.Candidate, now time.Time) protocol.Notification {
	value := cloneInt32(candidate.Value)
	return protocol.Notification{
		ID: id, AppName: candidate.AppName, AppIcon: candidate.AppIcon,
		DesktopEntry: candidate.DesktopEntry, Summary: candidate.Summary, Body: candidate.Body,
		Actions: append([]protocol.Action(nil), candidate.Actions...), Urgency: candidate.Urgency,
		Category: candidate.Category, Timestamp: now.UTC(), ExpireTimeoutMS: candidate.ExpireTimeout,
		Image: cloneImage(candidate.Image), Value: value, InlineReply: candidate.InlineReply,
		SenderLineage: append([]protocol.Process(nil), candidate.Sender.Lineage...),
	}
}

func candidateDuration(candidate notify.Candidate) time.Duration {
	switch candidate.ExpireTimeout {
	case -1:
		// The spec lets the server default vary by type. Critical alerts stay
		// until dismissed; every other urgency keeps the short default.
		if candidate.Urgency == protocol.UrgencyCritical {
			return 0
		}
		return DefaultTimeout
	case 0:
		return 0
	default:
		return time.Duration(candidate.ExpireTimeout) * time.Millisecond
	}
}

func (s *ownerState) close(id uint32, reason protocol.CloseReason) error {
	record, exists := s.records[id]
	if !exists {
		return ErrNotFound
	}
	delete(s.records, id)
	if record.producerKey != "" && s.producerIDs[record.producerKey] == id {
		delete(s.producerIDs, record.producerKey)
	}
	for i, orderedID := range s.order {
		if orderedID == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	s.publishDelta(protocol.Delta{Kind: protocol.DeltaClosed, ID: id, CloseReason: reason})
	if s.history != nil && !record.candidate.Transient && !record.candidate.Private {
		now := s.clock.Now()
		entry, removed, err := s.history.Add(historyEntry(record.notification, now), now)
		if err != nil {
			// The close has happened and been announced; history records it
			// but cannot undo it, so a failed write costs only the entry.
			log.Printf("sysc-notify: history: drop closed notification %d: %v", id, err)
			return nil
		}
		for _, removedID := range removed {
			s.publishDelta(protocol.Delta{Kind: protocol.DeltaHistoryRemoved, ID: removedID})
		}
		if entry.ID != 0 {
			s.publishDelta(protocol.Delta{Kind: protocol.DeltaHistoryAdded, History: cloneHistoryPointer(entry)})
		}
	}
	return nil
}

// historyEntry stamps the entry with its close time: retention measures how
// long ago a notification left, not how long it was shown.
func historyEntry(notification protocol.Notification, closedAt time.Time) protocol.HistoryEntry {
	return protocol.HistoryEntry{
		ID: notification.ID, AppName: notification.AppName, AppIcon: notification.AppIcon,
		DesktopEntry: notification.DesktopEntry, Summary: notification.Summary, Body: notification.Body,
		Urgency: notification.Urgency, Category: notification.Category, Timestamp: closedAt.UTC(),
		Image: cloneImage(notification.Image),
	}
}

func (s *ownerState) clearHistory() error {
	if s.history == nil {
		return errors.New("state: history unavailable")
	}
	removed, err := s.history.Clear()
	if err != nil {
		return err
	}
	if len(removed) != 0 {
		s.publishDelta(protocol.Delta{Kind: protocol.DeltaHistoryCleared})
	}
	return nil
}

func (s *ownerState) removeHistory(ids []uint32) error {
	if s.history == nil {
		return errors.New("state: history unavailable")
	}
	removed, err := s.history.Remove(ids)
	if err != nil {
		return err
	}
	for _, id := range removed {
		s.publishDelta(protocol.Delta{Kind: protocol.DeltaHistoryRemoved, ID: id})
	}
	return nil
}

func (s *ownerState) markHistorySeen(ids []uint32) error {
	if s.history == nil {
		return errors.New("state: history unavailable")
	}
	changed, err := s.history.MarkSeen(ids)
	if err != nil {
		return err
	}
	if len(changed) != 0 {
		s.publishDelta(protocol.Delta{Kind: protocol.DeltaHistorySeen, IDs: append([]uint32(nil), changed...)})
	}
	return nil
}

func (s *ownerState) invokeAction(id uint32, key string) error {
	record, exists := s.records[id]
	if !exists {
		return ErrNotFound
	}
	found := false
	for _, action := range record.notification.Actions {
		if action.Key == key {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("state: unknown action %q", key)
	}
	s.publish(Event{Action: &ActionEvent{ID: id, Key: key}})
	if !record.candidate.Resident {
		return s.close(id, protocol.CloseDismissed)
	}
	return nil
}

func (s *ownerState) submitReply(id uint32, text string) error {
	if err := (protocol.Command{Kind: protocol.CommandReply, ID: id, Text: text}).Validate(); err != nil {
		return err
	}
	record, exists := s.records[id]
	if !exists {
		return ErrNotFound
	}
	if !record.candidate.InlineReply {
		return errors.New("state: notification has no inline reply")
	}
	s.publish(Event{Reply: &ReplyEvent{ID: id, Text: text}})
	if !record.candidate.Resident {
		return s.close(id, protocol.CloseDismissed)
	}
	return nil
}

func (s *ownerState) evictCapacityVictim() {
	choose := func(match func(*record) bool) uint32 {
		for _, id := range s.order {
			if match(s.records[id]) {
				return id
			}
		}
		return 0
	}
	victim := choose(func(record *record) bool {
		return record.notification.Urgency != protocol.UrgencyCritical && record.duration > 0
	})
	if victim == 0 {
		victim = choose(func(record *record) bool { return record.notification.Urgency != protocol.UrgencyCritical })
	}
	if victim == 0 && len(s.order) > 0 {
		victim = s.order[0]
	}
	if victim != 0 {
		_ = s.close(victim, protocol.CloseUndefined)
	}
}

func (s *ownerState) publishDelta(delta protocol.Delta) {
	s.sequence++
	s.publish(Event{Sequence: s.sequence, Delta: &delta})
}

func (s *ownerState) publish(event Event) {
	if s.sink != nil {
		s.sink.Publish(event)
	}
}

func (s *ownerState) snapshot() protocol.Snapshot {
	snapshot := protocol.Snapshot{
		Sequence:  s.sequence,
		Active:    make([]protocol.Notification, 0, len(s.order)),
		Lifetimes: s.lifetimes(s.clock.Now()),
	}
	for _, id := range s.order {
		snapshot.Active = append(snapshot.Active, cloneNotification(s.records[id].notification))
	}
	if s.history != nil {
		snapshot.History = s.history.Entries()
	}
	return snapshot
}

func (s *ownerState) lifetimes(now time.Time) []protocol.Lifetime {
	lifetimes := make([]protocol.Lifetime, 0, len(s.order))
	for _, id := range s.order {
		lifetimes = append(lifetimes, s.lifetime(s.records[id], now))
	}
	return lifetimes
}

func (s *ownerState) lifetime(record *record, now time.Time) protocol.Lifetime {
	remaining := record.remaining
	if record.hasDeadline {
		remaining = max(0, record.deadline.Sub(now))
	}
	return protocol.Lifetime{
		ID:          record.notification.ID,
		DurationMS:  durationMilliseconds(record.duration),
		RemainingMS: durationMilliseconds(remaining),
		Running:     record.hasDeadline && record.duration > 0,
	}
}

func durationMilliseconds(duration time.Duration) uint32 {
	if duration <= 0 {
		return 0
	}
	return uint32((duration + time.Millisecond - 1) / time.Millisecond)
}

func cloneHistoryPointer(entry protocol.HistoryEntry) *protocol.HistoryEntry {
	cloned := entry
	cloned.Image = cloneImage(entry.Image)
	return &cloned
}

func cloneNotificationPointer(notification protocol.Notification) *protocol.Notification {
	cloned := cloneNotification(notification)
	return &cloned
}

func cloneNotification(notification protocol.Notification) protocol.Notification {
	notification.Actions = append([]protocol.Action(nil), notification.Actions...)
	notification.SenderLineage = append([]protocol.Process(nil), notification.SenderLineage...)
	notification.Image = cloneImage(notification.Image)
	notification.Value = cloneInt32(notification.Value)
	return notification
}

func cloneImage(image *protocol.Image) *protocol.Image {
	if image == nil {
		return nil
	}
	clone := *image
	clone.Data = append([]byte(nil), image.Data...)
	return &clone
}

func cloneInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
