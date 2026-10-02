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
	}

	for key, value := range request.Hints {
		switch key {
		case HintUrgency:
			urgency, ok := value.(uint8)
			if !ok || urgency > uint8(protocol.UrgencyCritical) {
				return Candidate{}, errors.New("notify: invalid urgency hint")
			}
			candidate.Urgency = protocol.Urgency(urgency)
		case HintTransient:
			var ok bool
			candidate.Transient, ok = value.(bool)
			if !ok {
				return Candidate{}, errors.New("notify: invalid transient hint")
			}
		case HintPrivate:
			var ok bool
			candidate.Private, ok = value.(bool)
			if !ok {
				return Candidate{}, errors.New("notify: invalid private hint")
			}
		case HintResident:
			var ok bool
			candidate.Resident, ok = value.(bool)
			if !ok {
				return Candidate{}, errors.New("notify: invalid resident hint")
			}
		case HintDesktopEntry:
			var ok bool
			candidate.DesktopEntry, ok = value.(string)
			if !ok {
				return Candidate{}, errors.New("notify: invalid desktop-entry hint")
			}
			if err := validateString("desktop entry", candidate.DesktopEntry, true); err != nil {
				return Candidate{}, err
			}
		case HintCategory:
			var ok bool
			candidate.Category, ok = value.(string)
			if !ok {
				return Candidate{}, errors.New("notify: invalid category hint")
			}
			if err := validateString("category", candidate.Category, true); err != nil {
				return Candidate{}, err
			}
		case HintValue:
			v, ok := value.(int32)
			if !ok || v < 0 || v > 100 {
				return Candidate{}, errors.New("notify: invalid value hint")
			}
			candidate.Value = &v
		case HintInlineReplyPlaceholder:
			placeholder, ok := value.(string)
			if !ok {
				return Candidate{}, errors.New("notify: invalid inline-reply hint")
			}
			if err := validateString("reply placeholder", placeholder, true); err != nil {
				return Candidate{}, err
			}
			candidate.InlineReply = true
			candidate.ReplyPlaceholder = placeholder
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
