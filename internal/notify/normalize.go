package notify

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

func Normalize(request Request) (Candidate, error) {
	if len(request.Hints) > protocol.MaxHints {
		return Candidate{}, errors.New("notify: too many hints")
	}
	if request.ExpireTimeout < -1 {
		return Candidate{}, errors.New("notify: invalid expiry timeout")
	}
	for name, value := range map[string]string{
		"app name": request.AppName, "app icon": request.AppIcon, "summary": request.Summary,
		"body": request.Body, "sender": request.Sender.Name,
	} {
		if err := validateString(name, value, true); err != nil {
			return Candidate{}, err
		}
	}
	if len(request.Actions)%2 != 0 || len(request.Actions)/2 > protocol.MaxActionPairs {
		return Candidate{}, errors.New("notify: invalid action list")
	}

	sender := request.Sender
	sender.Lineage = append([]protocol.Process(nil), request.Sender.Lineage...)
	candidate := Candidate{
		AppName: request.AppName, AppIcon: request.AppIcon, Summary: request.Summary, Body: request.Body,
		ReplacesID: request.ReplacesID, ExpireTimeout: request.ExpireTimeout,
		Urgency: protocol.UrgencyNormal, Sender: sender,
	}
	actionKeys := make(map[string]struct{}, len(request.Actions)/2)
	for i := 0; i < len(request.Actions); i += 2 {
		action := protocol.Action{Key: request.Actions[i], Label: request.Actions[i+1]}
		if err := action.Validate(); err != nil {
			return Candidate{}, fmt.Errorf("notify: action %d: %w", i/2, err)
		}
		if _, exists := actionKeys[action.Key]; exists {
			return Candidate{}, fmt.Errorf("notify: duplicate action key %q", action.Key)
		}
		actionKeys[action.Key] = struct{}{}
		candidate.Actions = append(candidate.Actions, action)
		if action.Key == ActionInlineReply {
			candidate.InlineReply = true
		}
	}

	for key, value := range request.Hints {
		// Optional hints are decorations, not the notification: a wrong type or
		// an out-of-range value drops that one hint and leaves the default, the
		// same soft path a malformed image hint takes. Structural problems
		// (hints count, core text, action list, expiry) still fail the call.
		switch key {
		case HintUrgency:
			if urgency, ok := value.(uint8); ok && urgency <= uint8(protocol.UrgencyCritical) {
				candidate.Urgency = protocol.Urgency(urgency)
			}
		case HintTransient:
			candidate.Transient, _ = value.(bool)
		case HintPrivate:
			candidate.Private, _ = value.(bool)
		case HintResident:
			candidate.Resident, _ = value.(bool)
		case HintDesktopEntry:
			if entry, ok := value.(string); ok && validateString("desktop entry", entry, true) == nil {
				candidate.DesktopEntry = entry
			}
		case HintCategory:
			if category, ok := value.(string); ok && validateString("category", category, true) == nil {
				candidate.Category = category
			}
		case HintValue:
			if v, ok := value.(int32); ok && v >= 0 && v <= 100 {
				candidate.Value = &v
			}
		case HintInlineReplyPlaceholder:
			if placeholder, ok := value.(string); ok && validateString("reply placeholder", placeholder, true) == nil {
				candidate.InlineReply = true
				candidate.ReplyPlaceholder = placeholder
			}
		}
	}
	applyImageHints(&candidate, request.Hints)
	return candidate, nil
}

// applyImageHints follows the Freedesktop image precedence: a present
// image-data hint hides image-path, and either hint replaces app_icon when
// it decodes. A malformed image is dropped and Notify still succeeds.
func applyImageHints(candidate *Candidate, hints map[string]any) {
	if raw, ok := hints[HintImageData]; ok {
		image, isImage := raw.(RawImage)
		if !isImage {
			candidate.ImageRejected = true
			return
		}
		decoded, err := normalizeImage(image)
		if err != nil {
			candidate.ImageRejected = true
			return
		}
		candidate.Image = decoded
		candidate.AppIcon = ""
		return
	}
	raw, ok := hints[HintImagePath]
	if !ok {
		return
	}
	path, isString := raw.(string)
	if !isString || validateString("image path", path, true) != nil {
		candidate.ImageRejected = true
		return
	}
	if path == "" {
		return
	}
	icon, ok := imagePathIcon(path)
	if !ok {
		candidate.ImageRejected = true
		return
	}
	// ponytail: reuse app_icon's name-or-absolute-path contract; add a distinct wire field only if consumers need source-specific precedence.
	candidate.AppIcon = icon
}

// imagePathIcon reads an image-path hint the way the spec defines it: a
// file:// URI, an absolute path, or the name of an icon in the theme, which
// is how notify-send -i sends it. A name carries no separator or scheme, so it
// cannot point outside the theme; any other relative path, and any URI of
// another scheme or host, is rejected.
func imagePathIcon(hint string) (string, bool) {
	if strings.HasPrefix(hint, "file://") {
		u, err := url.Parse(hint)
		if err != nil || (u.Host != "" && u.Host != "localhost") || !filepath.IsAbs(u.Path) || u.Path == "/" {
			return "", false
		}
		return u.Path, true
	}
	if filepath.IsAbs(hint) {
		return hint, true
	}
	if hint == "." || hint == ".." || strings.ContainsAny(hint, "/:") {
		return "", false
	}
	return hint, true
}

func validateString(name, value string, emptyOK bool) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("notify: %s is not valid UTF-8", name)
	}
	if (!emptyOK && value == "") || len(value) > protocol.MaxBodyBytes {
		return fmt.Errorf("notify: invalid %s length", name)
	}
	return nil
}
