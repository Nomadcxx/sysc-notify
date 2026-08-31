package protocol

import (
	"encoding/json"
	"time"
)

const (
	ProtocolMajor uint16 = 1
	ProtocolMinor uint16 = 0

	RolePresenter = "presenter"

	MaxActiveNotifications    = 128
	MaxBodyBytes              = 16 << 10
	MaxActionPairs            = 6
	MaxHints                  = 64
	MaxSourceImageDimension   = 4096
	MaxSourceImageBytes       = 16 << 20
	MaxWireImageLongEdge      = 512
	MaxWireImageBytes         = 1 << 20
	MaxLineageEntries         = 16
	MaxHistoryEntries         = 100
	MaxPresenterQueueMessages = 256
	MaxPresenterDecodedBytes  = 32 << 20
	MaxCommandQueue           = 64
)

const HistoryRetention = 7 * 24 * time.Hour

const (
	KindHello          = "hello"
	KindSnapshot       = "snapshot"
	KindAdded          = "added"
	KindReplaced       = "replaced"
	KindClosed         = "closed"
	KindHistoryAdded   = "history-added"
	KindHistoryRemoved = "history-removed"
	KindHistorySeen    = "history-seen"
	KindHistoryCleared = "history-cleared"
	KindCommand        = "command"
	KindReply          = "reply"
)

type Envelope struct {
	Kind      string          `json:"kind"`
	RequestID uint64          `json:"request_id,omitempty"`
	Sequence  uint64          `json:"sequence,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

type Hello struct {
	Major        uint16   `json:"major"`
	Minor        uint16   `json:"minor"`
	Role         string   `json:"role"`
	Capabilities []string `json:"capabilities"`
}

type Snapshot struct {
	Sequence uint64         `json:"sequence"`
	Active   []Notification `json:"active"`
	History  []HistoryEntry `json:"history"`
}

type Urgency uint8

const (
	UrgencyLow Urgency = iota
	UrgencyNormal
	UrgencyCritical
)

type Notification struct {
	ID              uint32    `json:"id"`
	AppName         string    `json:"app_name,omitempty"`
	AppIcon         string    `json:"app_icon,omitempty"`
	DesktopEntry    string    `json:"desktop_entry,omitempty"`
	Summary         string    `json:"summary"`
	Body            string    `json:"body,omitempty"`
	Actions         []Action  `json:"actions,omitempty"`
	Urgency         Urgency   `json:"urgency"`
	Category        string    `json:"category,omitempty"`
	Timestamp       time.Time `json:"timestamp"`
	ExpireTimeoutMS int32     `json:"expire_timeout_ms"`
	Image           *Image    `json:"image,omitempty"`
	Value           *int32    `json:"value,omitempty"`
	InlineReply     bool      `json:"inline_reply,omitempty"`
	SenderLineage   []Process `json:"sender_lineage,omitempty"`
}

type Action struct {
	Key   string `json:"key"`
	Label string `json:"label"`
}

type Process struct {
	PID       uint32 `json:"pid"`
	StartTime uint64 `json:"start_time"`
}

type Image struct {
	MediaType string `json:"media_type"`
	Width     uint32 `json:"width"`
	Height    uint32 `json:"height"`
	Data      []byte `json:"data"`
}

type HistoryEntry struct {
	ID           uint32    `json:"id"`
	Seen         bool      `json:"seen"`
	AppName      string    `json:"app_name,omitempty"`
	AppIcon      string    `json:"app_icon,omitempty"`
	DesktopEntry string    `json:"desktop_entry,omitempty"`
	Summary      string    `json:"summary"`
	Body         string    `json:"body,omitempty"`
	Urgency      Urgency   `json:"urgency"`
	Category     string    `json:"category,omitempty"`
	Timestamp    time.Time `json:"timestamp"`
	Image        *Image    `json:"image,omitempty"`
}

type CloseReason uint32

const (
	CloseExpired   CloseReason = 1
	CloseDismissed CloseReason = 2
	CloseRequested CloseReason = 3
	CloseUndefined CloseReason = 4
)

type DeltaKind string

const (
	DeltaAdded          DeltaKind = KindAdded
	DeltaReplaced       DeltaKind = KindReplaced
	DeltaClosed         DeltaKind = KindClosed
	DeltaHistoryAdded   DeltaKind = KindHistoryAdded
	DeltaHistoryRemoved DeltaKind = KindHistoryRemoved
	DeltaHistorySeen    DeltaKind = KindHistorySeen
	DeltaHistoryCleared DeltaKind = KindHistoryCleared
)

type Delta struct {
	Kind         DeltaKind     `json:"kind"`
	Notification *Notification `json:"notification,omitempty"`
	History      *HistoryEntry `json:"history,omitempty"`
	ID           uint32        `json:"id,omitempty"`
	IDs          []uint32      `json:"ids,omitempty"`
	CloseReason  CloseReason   `json:"close_reason,omitempty"`
}

type PresentationState string

const (
	PresentationHovered    PresentationState = "hovered"
	PresentationVisible    PresentationState = "visible"
	PresentationQueued     PresentationState = "queued"
	PresentationSuppressed PresentationState = "suppressed"
)

type Presentation struct {
	ID    uint32            `json:"id"`
	State PresentationState `json:"state"`
}

type CommandKind string

const (
	CommandPresentationRenew CommandKind = "presentation.renew"
	CommandAction            CommandKind = "action.invoke"
	CommandDismiss           CommandKind = "notification.dismiss"
	CommandReply             CommandKind = "notification.reply"
	CommandHistoryClear      CommandKind = "history.clear"
	CommandHistoryMarkSeen   CommandKind = "history.mark-seen"
	CommandDismissAll        CommandKind = "active.dismiss-all"
)

type Command struct {
	Kind          CommandKind    `json:"kind"`
	ID            uint32         `json:"id,omitempty"`
	IDs           []uint32       `json:"ids,omitempty"`
	ActionKey     string         `json:"action_key,omitempty"`
	Text          string         `json:"text,omitempty"`
	Presentations []Presentation `json:"presentations,omitempty"`
}

type ErrorCode string

const (
	ErrorInvalid     ErrorCode = "invalid"
	ErrorNotFound    ErrorCode = "not_found"
	ErrorStale       ErrorCode = "stale"
	ErrorBusy        ErrorCode = "busy"
	ErrorUnavailable ErrorCode = "unavailable"
)

type ProtocolError struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message,omitempty"`
}

type Reply struct {
	OK    bool           `json:"ok"`
	Error *ProtocolError `json:"error,omitempty"`
}
