package notify

import (
	"strings"
	"testing"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

func TestResolveID(t *testing.T) {
	live := map[uint32]struct{}{1: {}, 2: {}, 7: {}}
	for _, tc := range []struct {
		name       string
		replacesID uint32
		next       uint32
		wantID     uint32
		wantNext   uint32
	}{
		{name: "existing replacement", replacesID: 7, next: 1, wantID: 7, wantNext: 1},
		{name: "missing replacement allocates", replacesID: 9, next: 1, wantID: 3, wantNext: 4},
		{name: "new skips live and zero", next: 0, wantID: 3, wantNext: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, next := ResolveID(tc.replacesID, tc.next, live)
			if id != tc.wantID || next != tc.wantNext {
				t.Fatalf("ResolveID() = (%d, %d), want (%d, %d)", id, next, tc.wantID, tc.wantNext)
			}
		})
	}
}

func TestNormalizeFields(t *testing.T) {
	request := Request{
		AppName:       "Browser",
		AppIcon:       "browser",
		Summary:       "Download complete",
		Body:          "report.pdf",
		Actions:       []string{"default", "Open", "reply", "Reply"},
		ExpireTimeout: 5000,
		Hints: map[string]any{
			HintUrgency:                uint8(protocol.UrgencyCritical),
			HintTransient:              true,
			HintDesktopEntry:           "browser.desktop",
			HintCategory:               "transfer.complete",
			HintValue:                  int32(73),
			HintPrivate:                true,
			HintInlineReplyPlaceholder: "Type a response",
		},
		Sender: Sender{Name: ":1.42", PID: 42},
	}
	got, err := Normalize(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Actions) != 2 || got.Actions[1].Key != "reply" {
		t.Fatalf("Actions = %#v", got.Actions)
	}
	if got.Urgency != protocol.UrgencyCritical || !got.Transient || !got.Private {
		t.Fatalf("typed hints = %#v", got)
	}
	if got.DesktopEntry != "browser.desktop" || got.Category != "transfer.complete" {
		t.Fatalf("string hints = %#v", got)
	}
	if got.Value == nil || *got.Value != 73 || !got.InlineReply || got.ReplyPlaceholder != "Type a response" {
		t.Fatalf("interaction hints = %#v", got)
	}
	if got.ExpireTimeout != 5000 || got.Sender.PID != 42 {
		t.Fatalf("request fields = %#v", got)
	}
}

func TestNormalizeRejectsStructuralBounds(t *testing.T) {
	tests := map[string]Request{
		"odd actions":   {Summary: "ok", Actions: []string{"key"}},
		"seven actions": {Summary: "ok", Actions: fourteenActionStrings()},
		"large body":    {Summary: "ok", Body: strings.Repeat("x", protocol.MaxBodyBytes+1)},
		"many hints":    {Summary: "ok", Hints: manyHints(protocol.MaxHints + 1)},
		"bad timeout":   {Summary: "ok", ExpireTimeout: -2},
		"bad urgency":   {Summary: "ok", Hints: map[string]any{HintUrgency: uint8(9)}},
		"bad value":     {Summary: "ok", Hints: map[string]any{HintValue: int32(101)}},
	}
	for name, request := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Normalize(request); err == nil {
				t.Fatal("Normalize() accepted invalid request")
			}
		})
	}
}

func TestNormalizeDoesNotMutateRequestOrExistingStateOnError(t *testing.T) {
	actions := []string{"key"}
	hints := map[string]any{"unknown": "kept"}
	request := Request{ReplacesID: 7, Summary: "ok", Actions: actions, Hints: hints}
	existing := map[uint32]string{7: "old"}
	if _, err := Normalize(request); err == nil {
		t.Fatal("Normalize() accepted odd action list")
	}
	if existing[7] != "old" || len(request.Actions) != 1 || request.Hints["unknown"] != "kept" {
		t.Fatal("Normalize() mutated caller-owned state")
	}
}

func fourteenActionStrings() []string {
	values := make([]string, 0, 14)
	for i := 0; i < 7; i++ {
		values = append(values, "key", "label")
	}
	return values
}

func manyHints(n int) map[string]any {
	hints := make(map[string]any, n)
	for i := 0; i < n; i++ {
		hints[string(rune(i+1))] = i
	}
	return hints
}
