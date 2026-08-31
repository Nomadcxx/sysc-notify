package state

import (
	"errors"
	"time"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

func (s *ownerState) renew(generation uint64, presentations []protocol.Presentation, now time.Time) error {
	if generation == 0 {
		return errors.New("state: presenter generation is zero")
	}
	command := protocol.Command{Kind: protocol.CommandPresentationRenew, Presentations: presentations}
	if err := command.Validate(); err != nil {
		return err
	}
	for _, presentation := range presentations {
		if _, exists := s.records[presentation.ID]; !exists {
			return ErrNotFound
		}
	}
	if s.presenterGeneration != 0 && generation != s.presenterGeneration {
		s.clearPresentation(now)
	}
	s.presenterGeneration = generation
	s.leaseDeadline = now.Add(PresentationLease)

	states := make(map[uint32]protocol.PresentationState, len(presentations))
	for _, presentation := range presentations {
		states[presentation.ID] = presentation.State
	}
	for _, id := range s.order {
		state := protocol.PresentationSuppressed
		if reported, ok := states[id]; ok {
			state = reported
		}
		s.setPresentation(s.records[id], state, now)
	}
	return nil
}

func (s *ownerState) clearPresentation(now time.Time) {
	for _, id := range s.order {
		s.setPresentation(s.records[id], protocol.PresentationSuppressed, now)
	}
	s.presenterGeneration = 0
	s.leaseDeadline = time.Time{}
}

func (s *ownerState) setPresentation(record *record, next protocol.PresentationState, now time.Time) {
	if record.state == next {
		return
	}
	if record.hasDeadline {
		record.remaining = max(0, record.deadline.Sub(now))
		record.hasDeadline = false
	}
	previous := record.state
	record.state = next
	if record.duration == 0 {
		return
	}
	switch next {
	case protocol.PresentationQueued:
		if !record.displayed {
			record.remaining = record.duration
		}
	case protocol.PresentationHovered:
		record.displayed = true
	case protocol.PresentationVisible:
		if !record.displayed {
			record.displayed = true
			if previous == protocol.PresentationQueued {
				record.remaining = record.duration
			}
		}
		record.deadline = now.Add(record.remaining)
		record.hasDeadline = true
	case protocol.PresentationSuppressed:
		if record.remaining <= 0 {
			record.remaining = record.duration
		}
		record.deadline = now.Add(record.remaining)
		record.hasDeadline = true
	}
}

func (s *ownerState) handleTimer() {
	now := s.clock.Now()
	if !s.leaseDeadline.IsZero() && !s.leaseDeadline.After(now) {
		expiredAt := s.leaseDeadline
		s.clearPresentation(expiredAt)
	}
	for _, id := range append([]uint32(nil), s.order...) {
		record := s.records[id]
		if record != nil && record.hasDeadline && !record.deadline.After(now) {
			_ = s.close(id, protocol.CloseExpired)
		}
	}
}

func (s *ownerState) resetTimer() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
		s.timerC = nil
	}
	var next time.Time
	if !s.leaseDeadline.IsZero() {
		next = s.leaseDeadline
	}
	for _, id := range s.order {
		record := s.records[id]
		if record.hasDeadline && (next.IsZero() || record.deadline.Before(next)) {
			next = record.deadline
		}
	}
	if next.IsZero() {
		return
	}
	duration := next.Sub(s.clock.Now())
	if duration < 0 {
		duration = 0
	}
	s.timer = s.clock.NewTimer(duration)
	s.timerC = s.timer.C()
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) NewTimer(duration time.Duration) Timer {
	return systemTimer{Timer: time.NewTimer(duration)}
}

type systemTimer struct {
	*time.Timer
}

func (t systemTimer) C() <-chan time.Time { return t.Timer.C }
