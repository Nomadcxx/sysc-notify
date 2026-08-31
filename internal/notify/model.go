package notify

import "github.com/Nomadcxx/sysc-notify/protocol"

const (
	HintUrgency                = "urgency"
	HintTransient              = "transient"
	HintDesktopEntry           = "desktop-entry"
	HintCategory               = "category"
	HintValue                  = "value"
	HintPrivate                = "x-sysc-private"
	HintInlineReplyPlaceholder = "x-kde-reply-placeholder-text"
	HintImageData              = "image-data"
)

type Sender struct {
	Name string
	PID  uint32
}

type Request struct {
	AppName, AppIcon, Summary, Body string
	ReplacesID                      uint32
	Actions                         []string
	Hints                           map[string]any
	ExpireTimeout                   int32
	Sender                          Sender
}

type Candidate struct {
	AppName, AppIcon, Summary, Body string
	ReplacesID                      uint32
	Actions                         []protocol.Action
	Urgency                         protocol.Urgency
	ExpireTimeout                   int32
	Transient, Private              bool
	DesktopEntry, Category          string
	Value                           *int32
	InlineReply                     bool
	ReplyPlaceholder                string
	Image                           *protocol.Image
	ImageRejected                   bool
	Sender                          Sender
}

func ResolveID(replacesID, next uint32, live map[uint32]struct{}) (uint32, uint32) {
	if replacesID != 0 {
		if _, exists := live[replacesID]; exists {
			return replacesID, next
		}
	}
	if next == 0 {
		next = 1
	}
	for {
		if _, exists := live[next]; !exists {
			id := next
			next++
			if next == 0 {
				next = 1
			}
			return id, next
		}
		next++
		if next == 0 {
			next = 1
		}
	}
}
