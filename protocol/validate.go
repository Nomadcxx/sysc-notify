package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

func (e Envelope) Validate() error {
	if len(e.Payload) == 0 || !json.Valid(e.Payload) {
		return errors.New("protocol: envelope has invalid payload")
	}
	switch e.Kind {
	case KindHello:
		if e.RequestID != 0 || e.Sequence != 0 {
			return errors.New("protocol: hello has correlation fields")
		}
	case KindSnapshot, KindAdded, KindReplaced, KindClosed, KindHistoryAdded, KindHistoryRemoved,
		KindHistorySeen, KindHistoryCleared:
		if e.Sequence == 0 || e.RequestID != 0 {
			return errors.New("protocol: state message has invalid sequence")
		}
	case KindCommand, KindReply:
		if e.RequestID == 0 || e.Sequence != 0 {
			return errors.New("protocol: request message has invalid request ID")
		}
	default:
		return fmt.Errorf("protocol: unknown message kind %q", e.Kind)
	}
	return nil
}

func ValidateNextSequence(previous, next uint64) error {
	if previous == ^uint64(0) || next != previous+1 {
		return fmt.Errorf("protocol: sequence %d does not follow %d", next, previous)
	}
	return nil
}

func (h Hello) Validate(role string) error {
	if h.Major != ProtocolMajor {
		return fmt.Errorf("protocol: incompatible major %d", h.Major)
	}
	if h.Role != role {
		return fmt.Errorf("protocol: unexpected role %q", h.Role)
	}
	seen := make(map[string]struct{}, len(h.Capabilities))
	for _, capability := range h.Capabilities {
		if err := validateText("capability", capability, MaxBodyBytes, false); err != nil {
			return err
		}
		if _, ok := seen[capability]; ok {
			return fmt.Errorf("protocol: duplicate capability %q", capability)
		}
		seen[capability] = struct{}{}
	}
	return nil
}

func (s Snapshot) Validate() error {
	if s.Sequence == 0 {
		return errors.New("protocol: snapshot sequence is zero")
	}
	if len(s.Active) > MaxActiveNotifications || len(s.History) > MaxHistoryEntries {
		return errors.New("protocol: snapshot exceeds record limit")
	}
	active := make(map[uint32]struct{}, len(s.Active))
	for i := range s.Active {
		if err := s.Active[i].Validate(); err != nil {
			return fmt.Errorf("protocol: active[%d]: %w", i, err)
		}
		if _, exists := active[s.Active[i].ID]; exists {
			return fmt.Errorf("protocol: duplicate active ID %d", s.Active[i].ID)
		}
		active[s.Active[i].ID] = struct{}{}
	}
	history := make(map[uint32]struct{}, len(s.History))
	for i := range s.History {
		if err := s.History[i].Validate(); err != nil {
			return fmt.Errorf("protocol: history[%d]: %w", i, err)
		}
		if _, exists := history[s.History[i].ID]; exists {
			return fmt.Errorf("protocol: duplicate history ID %d", s.History[i].ID)
		}
		history[s.History[i].ID] = struct{}{}
	}
	return nil
}

func (n Notification) Validate() error {
	if n.ID == 0 {
		return errors.New("protocol: notification ID is zero")
	}
	if !n.Urgency.valid() {
		return fmt.Errorf("protocol: invalid urgency %d", n.Urgency)
	}
	if n.Timestamp.IsZero() {
		return errors.New("protocol: notification timestamp is zero")
	}
	for name, value := range map[string]string{
		"app name": n.AppName, "app icon": n.AppIcon, "desktop entry": n.DesktopEntry,
		"summary": n.Summary, "body": n.Body, "category": n.Category,
	} {
		if err := validateText(name, value, MaxBodyBytes, true); err != nil {
			return err
		}
	}
	if len(n.Actions) > MaxActionPairs {
		return errors.New("protocol: too many actions")
	}
	actions := make(map[string]struct{}, len(n.Actions))
	for i := range n.Actions {
		if err := n.Actions[i].Validate(); err != nil {
			return fmt.Errorf("protocol: action[%d]: %w", i, err)
		}
		if _, exists := actions[n.Actions[i].Key]; exists {
			return fmt.Errorf("protocol: duplicate action key %q", n.Actions[i].Key)
		}
		actions[n.Actions[i].Key] = struct{}{}
	}
	if n.Image != nil {
		if err := n.Image.Validate(); err != nil {
			return err
		}
	}
	if n.Value != nil && (*n.Value < 0 || *n.Value > 100) {
		return errors.New("protocol: value is outside 0..100")
	}
	if len(n.SenderLineage) > MaxLineageEntries {
		return errors.New("protocol: sender lineage is too long")
	}
	for _, process := range n.SenderLineage {
		if process.PID == 0 || process.StartTime == 0 {
			return errors.New("protocol: invalid sender process")
		}
	}
	return nil
}

func (a Action) Validate() error {
	if err := validateText("action key", a.Key, MaxBodyBytes, false); err != nil {
		return err
	}
	return validateText("action label", a.Label, MaxBodyBytes, false)
}

func (i Image) Validate() error {
	if i.MediaType != "image/png" {
		return fmt.Errorf("protocol: unsupported image media type %q", i.MediaType)
	}
	if i.Width == 0 || i.Height == 0 || i.Width > MaxWireImageLongEdge || i.Height > MaxWireImageLongEdge {
		return errors.New("protocol: invalid wire image dimensions")
	}
	if len(i.Data) == 0 || len(i.Data) > MaxWireImageBytes {
		return errors.New("protocol: invalid wire image size")
	}
	return nil
}

func (h HistoryEntry) Validate() error {
	if h.ID == 0 || h.Timestamp.IsZero() || !h.Urgency.valid() {
		return errors.New("protocol: invalid history identity")
	}
	for name, value := range map[string]string{
		"app name": h.AppName, "app icon": h.AppIcon, "desktop entry": h.DesktopEntry,
		"summary": h.Summary, "body": h.Body, "category": h.Category,
	} {
		if err := validateText(name, value, MaxBodyBytes, true); err != nil {
			return err
		}
	}
	if h.Image != nil {
		return h.Image.Validate()
	}
	return nil
}

func (d Delta) Validate() error {
	switch d.Kind {
	case DeltaAdded, DeltaReplaced:
		if d.Notification == nil {
			return errors.New("protocol: notification delta has no record")
		}
		return d.Notification.Validate()
	case DeltaClosed:
		if d.ID == 0 || !d.CloseReason.valid() {
			return errors.New("protocol: invalid closed delta")
		}
	case DeltaHistoryAdded:
		if d.History == nil {
			return errors.New("protocol: history delta has no record")
		}
		return d.History.Validate()
	case DeltaHistoryRemoved:
		if d.ID == 0 {
			return errors.New("protocol: history removal ID is zero")
		}
	case DeltaHistorySeen:
		return validateIDs(d.IDs, MaxHistoryEntries)
	case DeltaHistoryCleared:
		return nil
	default:
		return fmt.Errorf("protocol: invalid delta kind %q", d.Kind)
	}
	return nil
}

func (c Command) Validate() error {
	switch c.Kind {
	case CommandPresentationRenew:
		if len(c.Presentations) == 0 || len(c.Presentations) > MaxActiveNotifications {
			return errors.New("protocol: invalid presentation renewal")
		}
		seen := make(map[uint32]struct{}, len(c.Presentations))
		for _, presentation := range c.Presentations {
			if presentation.ID == 0 || !presentation.State.valid() {
				return errors.New("protocol: invalid presentation")
			}
			if _, exists := seen[presentation.ID]; exists {
				return errors.New("protocol: duplicate presentation ID")
			}
			seen[presentation.ID] = struct{}{}
		}
	case CommandAction:
		if c.ID == 0 {
			return errors.New("protocol: action ID is zero")
		}
		return validateText("action key", c.ActionKey, MaxBodyBytes, false)
	case CommandDismiss:
		if c.ID == 0 {
			return errors.New("protocol: dismiss ID is zero")
		}
	case CommandReply:
		if c.ID == 0 {
			return errors.New("protocol: reply ID is zero")
		}
		return validateText("reply text", c.Text, MaxBodyBytes, true)
	case CommandHistoryClear, CommandDismissAll:
		return nil
	case CommandHistoryMarkSeen:
		return validateIDs(c.IDs, MaxHistoryEntries)
	default:
		return fmt.Errorf("protocol: invalid command kind %q", c.Kind)
	}
	return nil
}

func (r Reply) Validate() error {
	if r.OK == (r.Error != nil) {
		return errors.New("protocol: reply must contain one outcome")
	}
	if r.Error == nil {
		return nil
	}
	if !r.Error.Code.valid() {
		return fmt.Errorf("protocol: invalid error code %q", r.Error.Code)
	}
	return validateText("error message", r.Error.Message, MaxBodyBytes, true)
}

func validateIDs(ids []uint32, limit int) error {
	if len(ids) == 0 || len(ids) > limit {
		return errors.New("protocol: invalid ID list length")
	}
	seen := make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			return errors.New("protocol: zero ID")
		}
		if _, exists := seen[id]; exists {
			return errors.New("protocol: duplicate ID")
		}
		seen[id] = struct{}{}
	}
	return nil
}

func validateText(name, value string, limit int, emptyOK bool) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("protocol: %s is not valid UTF-8", name)
	}
	if (!emptyOK && value == "") || len(value) > limit {
		return fmt.Errorf("protocol: invalid %s length", name)
	}
	return nil
}

func (u Urgency) valid() bool { return u <= UrgencyCritical }

func (r CloseReason) valid() bool { return r >= CloseExpired && r <= CloseUndefined }

func (s PresentationState) valid() bool {
	switch s {
	case PresentationHovered, PresentationVisible, PresentationQueued, PresentationSuppressed:
		return true
	default:
		return false
	}
}

func (c ErrorCode) valid() bool {
	switch c {
	case ErrorInvalid, ErrorNotFound, ErrorStale, ErrorBusy, ErrorUnavailable:
		return true
	default:
		return false
	}
}
