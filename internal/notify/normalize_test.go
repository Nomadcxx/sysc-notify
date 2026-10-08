package notify

import (
	"reflect"
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
			HintResident:               true,
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
	if got.Urgency != protocol.UrgencyCritical || !got.Transient || !got.Private || !got.Resident {
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

func TestNormalizeInlineReplyActionEnablesReplyWithoutHint(t *testing.T) {
	got, err := Normalize(Request{
		Summary: "Message from Alice",
		Actions: []string{"inline-reply", "Reply"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.InlineReply {
		t.Fatal("inline-reply action did not enable reply")
	}
	if got.ReplyPlaceholder != "" {
		t.Fatalf("ReplyPlaceholder = %q, want empty without the hint", got.ReplyPlaceholder)
	}
	if len(got.Actions) != 1 || got.Actions[0].Key != "inline-reply" || got.Actions[0].Label != "Reply" {
		t.Fatalf("Actions = %#v", got.Actions)
	}
}

func TestNormalizeInlineReplyHintRemainsPlaceholderSource(t *testing.T) {
	got, err := Normalize(Request{
		Summary: "Message from Alice",
		Actions: []string{"inline-reply", "Reply"},
		Hints:   map[string]any{HintInlineReplyPlaceholder: "Reply to Alice"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.InlineReply || got.ReplyPlaceholder != "Reply to Alice" {
		t.Fatalf("candidate = %#v", got)
	}
}

func TestNormalizeImagePathFallback(t *testing.T) {
	const path = "/usr/share/icons/hicolor/48x48/apps/example.png"
	got, err := Normalize(Request{Hints: map[string]any{"image-path": path}})
	if err != nil {
		t.Fatal(err)
	}
	if got.AppIcon != path {
		t.Fatalf("AppIcon = %q, want image-path %q", got.AppIcon, path)
	}
}

func TestNormalizeImagePathRequiresAbsolutePath(t *testing.T) {
	got, err := Normalize(Request{Hints: map[string]any{"image-path": "icons/example.png"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.AppIcon != "" || !got.ImageRejected {
		t.Fatalf("relative image-path was accepted: %#v", got)
	}
}

// The spec lets image-path name an icon in the theme, and notify-send -i
// sends it that way: a bare name is an icon name, not a relative path, so
// it passes through for the shell to resolve.
func TestNormalizeImagePathAcceptsAnIconName(t *testing.T) {
	got, err := Normalize(Request{Hints: map[string]any{"image-path": "spotify"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.AppIcon != "spotify" || got.ImageRejected {
		t.Fatalf("icon name = %#v, want AppIcon spotify", got)
	}
}

func TestNormalizeImagePathAcceptsAFileURI(t *testing.T) {
	got, err := Normalize(Request{Hints: map[string]any{"image-path": "file:///usr/share/pixmaps/example%20icon.png"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.AppIcon != "/usr/share/pixmaps/example icon.png" || got.ImageRejected {
		t.Fatalf("file URI = %#v, want its absolute path", got)
	}
}

func TestNormalizeImagePathRejectsWhatIsNeitherNameNorPath(t *testing.T) {
	for _, path := range []string{".", "..", "file://relative/icon.png", "file:///", "https://example.com/icon.png", "icons/../x.png"} {
		got, err := Normalize(Request{Hints: map[string]any{"image-path": path}})
		if err != nil {
			t.Fatal(err)
		}
		if got.AppIcon != "" || !got.ImageRejected {
			t.Fatalf("%q was accepted: %#v", path, got)
		}
	}
}

func TestNormalizeImagePathOverridesAppIcon(t *testing.T) {
	const path = "/usr/share/icons/hicolor/48x48/apps/example.png"
	got, err := Normalize(Request{AppIcon: "preferred", Hints: map[string]any{HintImagePath: path}})
	if err != nil {
		t.Fatal(err)
	}
	if got.AppIcon != path || got.Image != nil || got.ImageRejected {
		t.Fatalf("image-path = %#v, want AppIcon %q", got, path)
	}
}

func TestNormalizeImageDataClearsAppIcon(t *testing.T) {
	got, err := Normalize(Request{
		AppIcon: "firefox",
		Hints:   map[string]any{HintImageData: onePixel()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.AppIcon != "" || got.Image == nil || got.ImageRejected {
		t.Fatalf("image-data = %#v, want pixmap and empty AppIcon", got)
	}
}

func TestNormalizeImageDataHidesImagePath(t *testing.T) {
	const path = "/usr/share/icons/hicolor/48x48/apps/example.png"
	got, err := Normalize(Request{
		AppIcon: "firefox",
		Hints: map[string]any{
			HintImageData: onePixel(),
			HintImagePath: path,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.AppIcon != "" || got.Image == nil || got.ImageRejected {
		t.Fatalf("both image hints = %#v, want pixmap only", got)
	}
}

func TestMalformedImageDataKeepsAppIconAndIgnoresPath(t *testing.T) {
	got, err := Normalize(Request{
		AppIcon: "firefox",
		Hints: map[string]any{
			HintImageData: RawImage{Width: 1, Height: 1, RowStride: 3, BitsPerSample: 16, Channels: 3, Data: []byte{0, 0, 0}},
			HintImagePath: "/usr/share/icons/hicolor/48x48/apps/example.png",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.AppIcon != "firefox" || got.Image != nil || !got.ImageRejected {
		t.Fatalf("malformed image-data = %#v", got)
	}
}

func TestNormalizeEmptyImagePathKeepsAppIcon(t *testing.T) {
	got, err := Normalize(Request{AppIcon: "preferred", Hints: map[string]any{HintImagePath: ""}})
	if err != nil {
		t.Fatal(err)
	}
	if got.AppIcon != "preferred" || got.Image != nil || got.ImageRejected {
		t.Fatalf("empty image-path = %#v, want AppIcon preferred", got)
	}
}

func onePixel() RawImage {
	return RawImage{
		Width: 1, Height: 1, RowStride: 4, HasAlpha: true, BitsPerSample: 8, Channels: 4,
		Data: []byte{9, 8, 7, 255},
	}
}

func TestNormalizeBoundsIgnoredImagePath(t *testing.T) {
	path := strings.Repeat("x", protocol.MaxBodyBytes+1)
	got, err := Normalize(Request{AppIcon: "preferred", Hints: map[string]any{"image-path": path}})
	if err != nil {
		t.Fatal(err)
	}
	if got.AppIcon != "preferred" || !got.ImageRejected {
		t.Fatalf("invalid ignored image-path changed normalization: %#v", got)
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

func TestNormalizeAcceptsEmptyActionLabel(t *testing.T) {
	got, err := Normalize(Request{Summary: "ok", Actions: []string{"default", "", "open", "Open"}})
	if err != nil {
		t.Fatal(err)
	}
	want := []protocol.Action{{Key: "default", Label: ""}, {Key: "open", Label: "Open"}}
	if !reflect.DeepEqual(got.Actions, want) {
		t.Fatalf("actions = %#v, want %#v", got.Actions, want)
	}
}

func TestNormalizeRejectsEmptyActionKey(t *testing.T) {
	if _, err := Normalize(Request{Summary: "ok", Actions: []string{"", "Open"}}); err == nil {
		t.Fatal("Normalize() accepted an empty action key")
	}
}
