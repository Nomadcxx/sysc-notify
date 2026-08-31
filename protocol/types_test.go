package protocol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestSnapshotRoundTripAndValidate(t *testing.T) {
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	value := int32(73)
	want := Snapshot{
		Sequence: 4,
		Active: []Notification{{
			ID:              7,
			AppName:         "Browser",
			AppIcon:         "browser",
			DesktopEntry:    "browser.desktop",
			Summary:         "Download complete",
			Body:            "report.pdf",
			Actions:         []Action{{Key: "default", Label: "Open"}},
			Urgency:         UrgencyNormal,
			Category:        "transfer.complete",
			Timestamp:       now,
			ExpireTimeoutMS: 5000,
			Image:           &Image{MediaType: "image/png", Width: 32, Height: 16, Data: []byte("png")},
			Value:           &value,
			InlineReply:     true,
			SenderLineage:   []Process{{PID: 42, StartTime: 1234}},
		}},
		History: []HistoryEntry{{
			ID: 3, AppName: "Mail", Summary: "New mail", Urgency: UrgencyLow, Timestamp: now, Seen: true,
		}},
	}
	if err := want.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got Snapshot
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("round-trip Validate() = %v", err)
	}
	if got.Active[0].Value == nil || *got.Active[0].Value != value {
		t.Fatalf("round-trip value = %v, want %d", got.Active[0].Value, value)
	}
}

func TestHelloValidation(t *testing.T) {
	valid := Hello{Major: ProtocolMajor, Minor: ProtocolMinor, Role: RolePresenter, Capabilities: []string{"persistence", "actions"}}
	if err := valid.Validate(RolePresenter); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Hello){
		"wrong major":          func(h *Hello) { h.Major++ },
		"wrong role":           func(h *Hello) { h.Role = "diagnostic" },
		"duplicate capability": func(h *Hello) { h.Capabilities = append(h.Capabilities, "actions") },
		"empty capability":     func(h *Hello) { h.Capabilities = append(h.Capabilities, "") },
	} {
		t.Run(name, func(t *testing.T) {
			got := valid
			got.Capabilities = append([]string(nil), valid.Capabilities...)
			mutate(&got)
			if err := got.Validate(RolePresenter); err == nil {
				t.Fatal("Validate() accepted invalid hello")
			}
		})
	}
}

func TestEnvelopeValidation(t *testing.T) {
	for _, envelope := range []Envelope{
		{Kind: KindHello, Payload: json.RawMessage(`{}`)},
		{Kind: KindSnapshot, Sequence: 1, Payload: json.RawMessage(`{}`)},
		{Kind: KindAdded, Sequence: 2, Payload: json.RawMessage(`{}`)},
		{Kind: KindCommand, RequestID: 1, Payload: json.RawMessage(`{}`)},
		{Kind: KindReply, RequestID: 1, Payload: json.RawMessage(`{}`)},
	} {
		if err := envelope.Validate(); err != nil {
			t.Fatalf("%q Validate() = %v", envelope.Kind, err)
		}
	}
	for _, envelope := range []Envelope{
		{Kind: "future", Payload: json.RawMessage(`{}`)},
		{Kind: KindSnapshot, Payload: json.RawMessage(`{}`)},
		{Kind: KindCommand, Payload: json.RawMessage(`{}`)},
		{Kind: KindHello},
	} {
		if err := envelope.Validate(); err == nil {
			t.Fatalf("%+v accepted", envelope)
		}
	}
}

func TestValidateNextSequence(t *testing.T) {
	if err := ValidateNextSequence(9, 10); err != nil {
		t.Fatal(err)
	}
	for _, next := range []uint64{0, 9, 11} {
		if err := ValidateNextSequence(9, next); err == nil {
			t.Fatalf("ValidateNextSequence(9, %d) accepted", next)
		}
	}
}

func TestNotificationBounds(t *testing.T) {
	valid := Notification{ID: 1, Summary: "ok", Urgency: UrgencyNormal, Timestamp: time.Now().UTC()}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*Notification){
		"zero id":          func(n *Notification) { n.ID = 0 },
		"body":             func(n *Notification) { n.Body = strings.Repeat("x", MaxBodyBytes+1) },
		"actions":          func(n *Notification) { n.Actions = make([]Action, MaxActionPairs+1) },
		"urgency":          func(n *Notification) { n.Urgency = Urgency(9) },
		"value below zero": func(n *Notification) { v := int32(-1); n.Value = &v },
		"value above 100":  func(n *Notification) { v := int32(101); n.Value = &v },
		"lineage":          func(n *Notification) { n.SenderLineage = make([]Process, MaxLineageEntries+1) },
		"invalid UTF-8":    func(n *Notification) { n.Summary = string([]byte{0xff}) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			got := valid
			mutate(&got)
			if err := got.Validate(); err == nil {
				t.Fatal("Validate() accepted invalid notification")
			}
		})
	}
}

func TestImageValidationUsesOverflowSafeBounds(t *testing.T) {
	for name, image := range map[string]Image{
		"zero width":   {MediaType: "image/png", Height: 1, Data: []byte{1}},
		"wide":         {MediaType: "image/png", Width: MaxWireImageLongEdge + 1, Height: 1, Data: []byte{1}},
		"tall":         {MediaType: "image/png", Width: 1, Height: MaxWireImageLongEdge + 1, Data: []byte{1}},
		"large data":   {MediaType: "image/png", Width: 1, Height: 1, Data: make([]byte, MaxWireImageBytes+1)},
		"wrong format": {MediaType: "image/jpeg", Width: 1, Height: 1, Data: []byte{1}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := image.Validate(); err == nil {
				t.Fatal("Validate() accepted invalid image")
			}
		})
	}
}

func TestDeltaCommandAndReplyValidation(t *testing.T) {
	n := Notification{ID: 1, Summary: "ok", Urgency: UrgencyNormal, Timestamp: time.Now().UTC()}
	h := HistoryEntry{ID: 1, Summary: "ok", Urgency: UrgencyNormal, Timestamp: time.Now().UTC()}
	for _, delta := range []Delta{
		{Kind: DeltaAdded, Notification: &n},
		{Kind: DeltaReplaced, Notification: &n},
		{Kind: DeltaClosed, ID: 1, CloseReason: CloseDismissed},
		{Kind: DeltaHistoryAdded, History: &h},
		{Kind: DeltaHistoryRemoved, ID: 1},
		{Kind: DeltaHistorySeen, IDs: []uint32{1}},
		{Kind: DeltaHistoryCleared},
	} {
		if err := delta.Validate(); err != nil {
			t.Fatalf("%q Validate() = %v", delta.Kind, err)
		}
	}

	for _, command := range []Command{
		{Kind: CommandPresentationRenew, Presentations: []Presentation{{ID: 1, State: PresentationVisible}}},
		{Kind: CommandAction, ID: 1, ActionKey: "default"},
		{Kind: CommandDismiss, ID: 1},
		{Kind: CommandReply, ID: 1, Text: "answer"},
		{Kind: CommandHistoryClear},
		{Kind: CommandHistoryMarkSeen, IDs: []uint32{1}},
		{Kind: CommandDismissAll},
	} {
		if err := command.Validate(); err != nil {
			t.Fatalf("%q Validate() = %v", command.Kind, err)
		}
	}

	if err := (Reply{OK: true}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (Reply{Error: &ProtocolError{Code: ErrorBusy, Message: "queue full"}}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (Reply{OK: true, Error: &ProtocolError{Code: ErrorBusy}}).Validate(); err == nil {
		t.Fatal("Reply.Validate() accepted success with an error")
	}
}

func TestTextValidationRejectsInvalidUTF8(t *testing.T) {
	if utf8.ValidString(string([]byte{0xff})) {
		t.Fatal("test setup produced valid UTF-8")
	}
	if err := (Action{Key: string([]byte{0xff}), Label: "label"}).Validate(); err == nil {
		t.Fatal("Action.Validate() accepted invalid UTF-8")
	}
}
