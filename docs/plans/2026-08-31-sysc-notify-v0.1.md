# sysc-notify v0.1 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ship the notification daemon, its exported shell protocol, bounded disk history, and the
release gates required by sysc-shell Milestone 5.

**Architecture:** One state owner serializes active records, expiry, close reasons, presentation
leases, and history insertion. A D-Bus adapter and one authenticated presenter connection send commands
to that owner. The exported `protocol` package contains only standard-library wire types and framing;
the daemon alone imports the D-Bus dependency.

**Tech Stack:** Go 1.26, `github.com/godbus/dbus/v5`, freedesktop Notifications 1.3, Unix sockets,
length-prefixed JSON, standard-library image and persistence packages.

**Specs:**
- `docs/plans/2026-08-27-sysc-notify-design.md`
- `docs/plans/2026-08-30-sysc-notify-persistence-design.md`
- sysc-shell `docs/plans/2026-08-31-notifications-and-tray-integration-design.md`

---

## Global constraints

- `sysc-notify` alone owns IDs, replacement, expiry, active state, close reasons, and history.
- D-Bus calls never wait for a shell surface or block on a presenter writer.
- The `protocol` package imports only the Go standard library.
- Bound data before allocation or copying. One malformed record cannot remove a valid sibling.
- Active notifications are memory-only. Eligible closed records persist without actions or PIDs.
- Use one presenter generation. Unknown command outcomes are not retried.
- Do not add sound, remote body images, rule editors, or multiple frontend roles.
- Run race tests for every task that changes concurrent state.
- Tag `v0.1.0-rc.1` only after the service gate. Tag `v0.1.0` only after shell qualification.
- No local `replace` directive may appear in a release commit.

### Fixed bounds

| Resource | Limit |
|---|---:|
| Active records | 128 |
| Body | 16 KiB |
| Action pairs | 6 |
| Hints | 64 |
| Raw source image | 4096 × 4096 and 16 MiB decoded |
| Wire image | 512 px long edge and 1 MiB |
| PID lineage | 16 entries |
| History | 100 entries, 7 days |
| Wire frame | 16 MiB |
| Presenter outbound queue | 256 messages / 32 MiB decoded |
| Command writer queue | 64 commands |

---

### Task 1: Module and standard-library protocol package

**Files:**
- Create: `go.mod`
- Create: `protocol/frame.go`, `protocol/types.go`, `protocol/validate.go`
- Test: `protocol/frame_test.go`, `protocol/types_test.go`, `protocol/fuzz_test.go`

**Step 1: Create the module**

```bash
go mod init github.com/Nomadcxx/sysc-notify
go get github.com/godbus/dbus/v5@latest
```

Pin the resolved D-Bus version in `go.mod` and `go.sum`. Do not add another dependency.

**Step 2: Write failing framing tests**

Cover fragmented reads, several frames in one read, zero length, a declared length above 16 MiB,
truncation, trailing JSON, duplicate keys, unknown message kinds, missing required fields, invalid
enums, and fuzz input that must not panic or allocate beyond the declared cap.

Wire framing is four-byte unsigned big-endian length followed by one JSON object:

```go
const MaxFrameSize uint32 = 16 << 20

func ReadFrame(r io.Reader) ([]byte, error)
func WriteFrame(w io.Writer, payload []byte) error
func DecodeStrict(data []byte, dst any) error
```

`DecodeStrict` token-scans each object to reject duplicate keys before decoding with
`DisallowUnknownFields` for the fixed envelope. Payload structs may ignore documented future fields.

Run: `go test ./protocol/ -run 'Frame|Decode' -v`

Expected: FAIL.

**Step 3: Define the wire contract**

```go
const (
	ProtocolMajor = 1
	ProtocolMinor = 0
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

type PresentationState string

const (
	PresentationHovered    PresentationState = "hovered"
	PresentationVisible    PresentationState = "visible"
	PresentationQueued     PresentationState = "queued"
	PresentationSuppressed PresentationState = "suppressed"
)
```

Define immutable wire records, image metadata, deltas, commands, replies, and typed error codes for:

- presenter renewal and aggregate presentation state;
- action, dismiss, reply, `history.clear`, `history.mark-seen`, and `active.dismiss-all`;
- snapshot, added, replaced, closed, history-added, history-removed, history-seen, and
  history-cleared.

Export all numeric bounds from `protocol`. Validate major equality, minor compatibility, unique
capabilities, monotonic sequence, non-zero request IDs, enum values, UTF-8, sizes, and image metadata.

Run:

```bash
go test -race -count=1 ./protocol/
go list -deps ./protocol | rg -v '^(github.com/Nomadcxx/sysc-notify/protocol|[a-zA-Z0-9_/.-]+)$' || true
```

Then inspect `go list -deps ./protocol`; every entry must be a standard-library package or the
protocol package itself.

**Step 4: Commit**

```bash
git add go.mod go.sum protocol/
git commit -m "feat(protocol): define notification wire contract"
```

---

### Task 2: Bounded notification normalization

**Files:**
- Create: `internal/notify/model.go`, `internal/notify/normalize.go`, `internal/notify/image.go`
- Test: `internal/notify/normalize_test.go`, `internal/notify/image_test.go`

**Step 1: Write failing table tests**

Test:

- new ID allocation skips zero and live IDs;
- an existing non-zero replacement ID is reused;
- a missing replacement target allocates a new ID;
- six action pairs pass and a seventh fails without mutating state;
- oversized body and too many hints fail before copying;
- invalid optional image data produces a text-only record;
- row stride, channel count, bits-per-sample, dimensions, and decoded length use overflow-safe checks;
- replacing with a structurally invalid request leaves the old record unchanged;
- urgency, timeout, `transient`, `desktop-entry`, `category`, `value`,
  `x-sysc-private`, and inline-reply hints normalize to typed fields.

Use a transport-neutral input type:

```go
type Request struct {
	AppName, AppIcon, Summary, Body string
	ReplacesID                      uint32
	Actions                         []string
	Hints                           map[string]any
	ExpireTimeout                   int32
	Sender                          Sender
}

func Normalize(Request) (Candidate, error)
```

The D-Bus adapter converts variants to this type in Task 5.

**Step 2: Implement minimal normalization**

Copy only after bounds pass. Normalize action arrays to key/label pairs. Treat malformed optional images
as absent and return a diagnostic flag; reject structural request errors. Downscale accepted source
images to the wire cap with a bounded pure-Go nearest-neighbour pass and PNG encoding. Keep original
bytes out of active state after normalization.

Run:

```bash
go test -race -count=1 ./internal/notify/
```

Expected: PASS.

**Step 3: Commit**

```bash
git add internal/notify/
git commit -m "feat(notify): normalize bounded requests"
```

---

### Task 3: Active-state and lifetime owner

**Files:**
- Create: `internal/state/owner.go`, `internal/state/expiry.go`
- Test: `internal/state/owner_test.go`, `internal/state/expiry_test.go`

Use one owner goroutine with a command channel and one timer for the next deadline. D-Bus and IPC
goroutines receive immutable results and never mutate records.

**Step 1: Write failing state-machine tests**

Cover:

- add and replacement deltas are sequenced;
- `CloseNotification`, expiry, dismiss, action, reply, replacement, and capacity eviction emit one
  close event with the correct freedesktop reason;
- action emits `ActionInvoked` before optional close behavior;
- oldest-capacity victim order is finite non-critical, non-critical, then oldest;
- queued before first display has no running deadline;
- first visible display starts the full duration;
- hovered pauses remaining duration;
- visible resumes remaining duration;
- suppressed starts a never-displayed queued timeout and resumes a displayed timeout;
- presentation precedence is `hovered > visible > queued > suppressed`;
- renewals every two seconds hold state and six seconds without renewal clears every pause;
- presenter replacement and socket close clear holds immediately;
- replacement applies its timeout under the current aggregate state;
- active records never enter the history snapshot.

Inject a manual clock:

```go
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}
```

This is the one test seam for deterministic expiry; production uses a small stdlib adapter.

**Step 2: Implement the owner**

```go
type Owner struct {
	cmd  chan command
	done chan struct{}
}

func Start(clock Clock, sink Sink) *Owner
func (o *Owner) Do(context.Context, Command) (Result, error)
func (o *Owner) Snapshot(context.Context) (protocol.Snapshot, error)
func (o *Owner) Close() error
```

The owner keeps `map[uint32]*record`, insertion order, one expiry heap, history supplied by Task 4,
presenter generation, and sequence. It publishes immutable deltas to a bounded sink. It never blocks on
a slow presenter; overflow tells the IPC server to drop that generation.

Use `protocol.PresentationState` directly. The shell sends one already-aggregated state per active ID;
the service does not infer outputs.

Run: `go test -race -count=1 ./internal/state/`

Expected: PASS.

**Step 3: Commit**

```bash
git add internal/state/
git commit -m "feat(state): own notification lifetime"
```

---

### Task 4: Durable closed history

**Files:**
- Create: `internal/history/store.go`, `internal/history/schema.go`, `internal/history/image.go`
- Test: `internal/history/store_test.go`, `internal/history/recovery_test.go`
- Modify: `internal/state/owner.go`, `internal/state/owner_test.go`

**Step 1: Amend the persisted schema in tests**

Use a versioned top-level object, not a bare array:

```json
{"version":1,"entries":[{"id":42,"seen":false,"app_name":"Firefox","summary":"Done","timestamp":"2026-08-30T10:15:00Z"}]}
```

Persist no actions, reply tokens, sender PIDs, lineage, or private/transient records. Skip
`transient=true` and `x-sysc-private=true`; do not interpret Canonical or GNOME synchronization
hints as privacy flags.

Test:

- restart loads history and leaves active empty;
- the 101st entry evicts the oldest;
- entries older than seven days leave on startup and the 60-second sweep;
- `seen` survives restart and `mark-seen` is idempotent;
- `history.clear` does not dismiss active records;
- `active.dismiss-all` does not clear closed history;
- images are at most 96 px on the long edge, content-addressed, and written once;
- corrupt or future-schema JSON is renamed to a quarantine path and never overwritten;
- orphan removal runs only after a valid committed history supplies the reference set;
- an interrupted temporary write preserves the last committed file.

**Step 2: Implement crash-safe writes**

Resolve `XDG_STATE_HOME`, falling back to the current user's home plus `.local/state`. Refuse symlink
components and create directories as `0700`, files as `0600`.

For a commit: write a unique temporary file in the destination directory, `Sync`, close, rename over
`history.json`, open and `Sync` the directory. Apply the same write-once rule to PNG sidecars.
Quarantine invalid files with a timestamp and random suffix before starting empty.

The state owner calls history insertion only after an eligible record closes. History methods return the
new immutable entry/removal deltas for the same state sequence.

Run:

```bash
go test -race -count=1 ./internal/history/ ./internal/state/
```

Expected: PASS.

**Step 3: Commit**

```bash
git add internal/history/ internal/state/
git commit -m "feat(history): persist bounded closed records"
```

---

### Task 5: Freedesktop Notifications D-Bus adapter

**Files:**
- Create: `internal/fdo/server.go`, `internal/fdo/convert.go`
- Test: `internal/fdo/server_test.go`, `internal/fdo/convert_test.go`

**Step 1: Write failing private-bus tests**

Under `dbus-run-session`, call the exported interface rather than helpers:

- acquire `org.freedesktop.Notifications` once and fail on name loss;
- `GetServerInformation` returns stable name, vendor, version, and spec version;
- `Notify` add/replace behavior matches 1.3;
- `CloseNotification` distinguishes missing IDs without corrupting state;
- expiry and user dismiss emit one `NotificationClosed` with reasons 1, 2, 3, or 4 as applicable;
- action and inline reply emit the expected D-Bus signals;
- malformed variants return a D-Bus error and preserve existing records;
- `Notify` completes when no presenter is connected.

**Step 2: Implement the adapter**

Use `github.com/godbus/dbus/v5` only here and in integration fixtures. Convert D-Bus variants to
`notify.Request`, query `org.freedesktop.DBus.GetConnectionUnixProcessID`, submit synchronously to
the state owner, and translate typed errors to stable D-Bus errors.

`GetCapabilities` returns only qualified behavior. The v0.1 candidate may advertise `body`,
`actions`, `body-markup`, `persistence`, and inline reply only after their fake-presenter
end-to-end tests pass. Do not advertise sound, action icons, activation tokens, or hyperlinks without a
qualified opener.

Run:

```bash
dbus-run-session -- go test -race -count=1 ./internal/fdo/
```

Expected: PASS.

**Step 3: Commit**

```bash
git add internal/fdo/
git commit -m "feat(dbus): serve freedesktop notifications"
```

---

### Task 6: Authenticated presenter server

**Files:**
- Create: `internal/presenter/server.go`, `internal/presenter/connection.go`
- Test: `internal/presenter/server_test.go`, `internal/presenter/connection_test.go`
- Modify: `internal/state/owner.go`

**Step 1: Write failing transport and recovery tests**

Test the real Unix socket:

- runtime directory is `0700`, socket is `0600`, and symlink or wrong-owner paths fail;
- `SO_PEERCRED` UID mismatch fails before hello;
- protocol major mismatch and missing required capability fail;
- a valid presenter receives snapshot sequence N then N+1 deltas;
- a second valid presenter replaces the first generation and clears its holds;
- a queue above 256 messages or 32 MiB closes only that connection;
- malformed, duplicate, stale-sequence, or unknown messages close the generation;
- every command gets one matching reply;
- duplicate request IDs fail;
- disconnect clears presentation leases;
- reconnect gets a fresh snapshot including history;
- actions, menu-like retries, and replies with unknown outcomes are not retried.

**Step 2: Implement one presenter generation**

Create `$XDG_RUNTIME_DIR/sysc-notify/presenter.v1.sock`. One reader validates frames and submits
commands. One serialized writer owns frame output. Give the writer a 256-message/32-MiB bound.
Connection generation scopes request IDs and presentation renewals.

Handshake exchanges major, minor, role `presenter`, and capabilities. Send one validated snapshot,
record its sequence as the baseline, then forward ordered deltas. Any gap or overflow closes the socket;
the shell reconnects for a new snapshot.

Commands map to state-owner requests:

- presentation renewal and per-ID aggregate state;
- action, dismiss, and inline reply;
- `history.clear`, `history.mark-seen`, and `active.dismiss-all`.

Never block a D-Bus call on the writer. Use a non-blocking sink; disconnect the slow presenter when full.

Run: `go test -race -count=1 ./internal/presenter/ ./internal/state/`

Expected: PASS.

**Step 3: Commit**

```bash
git add internal/presenter/ internal/state/
git commit -m "feat(presenter): stream notification state"
```

---

### Task 7: Bounded sender lineage

**Files:**
- Create: `internal/sender/lineage.go`
- Test: `internal/sender/lineage_test.go`
- Modify: `internal/fdo/server.go`

**Step 1: Write failing proc-fixture tests**

Cover a normal chain, PID reuse via mismatched process start time, cycles, disappearing processes,
permission errors, malformed `stat`, and a chain longer than 16. No test reads the host's real `/proc`.

**Step 2: Implement**

Read the sender PID from the bus daemon, then capture at most 16 `{pid,start_time}` pairs from an
injected proc root. Parse `/proc/<pid>/stat` by locating the last `)` before numbered fields so spaces
and parentheses in command names remain valid. Stop safely on any ambiguity.

Lineage remains active-memory metadata and may travel in the active snapshot for conservative shell
matching. Do not write it to history.

Run: `go test -race -count=1 ./internal/sender/ ./internal/fdo/`

Expected: PASS.

**Step 3: Commit**

```bash
git add internal/sender/ internal/fdo/
git commit -m "feat(sender): capture bounded process lineage"
```

---

### Task 8: Process wiring, shutdown, and service gate

**Files:**
- Create: `cmd/sysc-notify/main.go`
- Create: `internal/app/run.go`, `internal/app/run_test.go`
- Create: `tests/integration/dbus_test.go`, `tests/integration/presenter_test.go`
- Modify: `README.md`

**Step 1: Wire one cancellable process**

Startup order:

1. validate and create state/runtime paths;
2. load and validate history;
3. start the state owner;
4. bind the presenter socket;
5. connect to D-Bus, export the object, then acquire the name;
6. publish readiness.

On SIGINT/SIGTERM: stop accepting, close presenter, release the D-Bus name, flush committed history,
stop the owner, and remove the socket. A partial startup unwinds in reverse order.

**Step 2: Add cross-process tests**

Use `dbus-run-session` and a built daemon binary. Exercise add, replace, action, reply, dismiss,
expiry, presenter loss/reconnect, service restart, corrupt history, private records, and a slow
presenter. Assert signal order, snapshot sequences, socket modes, and clean shutdown.

Fuzz for at least the default test duration:

```bash
go test ./protocol/ ./internal/notify/ ./internal/presenter/ -fuzz=Fuzz -fuzztime=10s
```

**Step 3: Run the service release-candidate gate**

```bash
gofmt -w .
test -z "$(gofmt -l .)"
go vet ./...
go test -race -count=1 ./...
dbus-run-session -- go test -race -count=1 ./tests/integration/
go list -m all
git diff --exit-code -- go.mod go.sum
```

Expected: all tests pass, one external module is pinned, no `replace` appears, and the worktree is
clean after committing.

**Step 4: Commit**

```bash
git add cmd/ internal/ tests/ README.md
git commit -m "feat: wire notification service"
```

---

### Task 9: Release-candidate and stable qualification

**Files:**
- Create: `docs/plans/2026-08-31-sysc-notify-v0.1-completion.md`

**Step 1: Freeze the protocol candidate**

Record the protocol constants, fixture hashes, full gate output, and known limits. From a clean commit:

```bash
git tag -s v0.1.0-rc.1 -m "sysc-notify v0.1.0-rc.1"
git push origin redesign/v0.1 v0.1.0-rc.1
```

Do not tag if signing, push authority, or the clean gate is unavailable; record the exact blocker.

**Step 2: Qualify with sysc-shell 5A**

The shell must pin the candidate module without a local replacement. Run the two-output live Niri matrix:
popup display, replace, default and named action, inline reply, swipe, countdown pause/resume, queued
first display, DND, center suppression, history seen/clear, dismiss-active, service and shell restart,
presenter lease expiry, malformed peer, and 60 minutes idle.

Any wire change requires `v0.1.0-rc.2` and an updated shell pin. Do not move the existing candidate tag.

**Step 3: Tag stable after qualification**

Rerun Task 8's clean gate on the exact qualified commit, update the completion document, then:

```bash
git tag -s v0.1.0 -m "sysc-notify v0.1.0"
git push origin v0.1.0
```

Expected: stable points at the protocol qualified by shell 5A.
