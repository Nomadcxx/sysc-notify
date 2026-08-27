# sysc-notify Design

Date: 2026-08-27
Status: Approved

## Purpose

`sysc-notify` is the single owner of `org.freedesktop.Notifications` for the sysc desktop. Its process
lifetime does not depend on a bar or popup surface, so applications can submit notifications while
`sysc-shell` restarts.

## Service boundary

`sysc-notify` owns D-Bus name acquisition, protocol methods and signals, notification IDs, replacement,
expiry, active state, bounded history, sender metadata, and the connection to presentation clients.

`sysc-shell` owns Wayland surfaces, visual policy, interaction, and Niri state. It receives immutable
notification snapshots and returns action, dismiss, and activation requests. The notification service
then emits the specification-required D-Bus signal.

The first implementation has one presentation client. The protocol must tolerate zero clients and a
client reconnect, but it will not include a general multi-frontend subscription system.

## Freedesktop behavior

Follow notification specification 1.3. `Notify` returns the supplied non-zero replacement ID when that
notification exists; otherwise it allocates a new non-zero ID. `CloseNotification`, expiry, user dismiss,
and undefined removal paths emit the correct `NotificationClosed` reason. User actions emit
`ActionInvoked`; activation tokens remain unavailable until `sysc-shell` supplies one through
`xdg_activation_v1`.

Advertised capabilities describe shipped behavior. The service does not claim body markup, persistence,
sound, action icons, or activation tokens before their end-to-end paths work.

## Trust boundaries

Bound string lengths, action count, hint count, raw image dimensions, row stride, decoded byte count,
active count, history count, and per-client request rate before allocating or copying large values.
Validate raw image metadata with overflow-safe arithmetic. Reject malformed values without losing valid
notifications already held by the service.

The service queries the bus daemon for the sender's Unix PID. It may record a bounded `/proc` parent
lineage in ephemeral state. It will not use `BecomeMonitor`, persist process IDs, or invoke Niri.

## Shell IPC

Use a private Unix socket with peer-credential checks and a versioned length-bounded protocol. The shell
receives an initial snapshot followed by ordered changes. If it falls behind, the service drops the
connection; reconnecting and requesting a fresh snapshot is simpler than maintaining an unbounded event
queue.

The service must complete `Notify` without waiting for the shell. Shell absence affects presentation,
not D-Bus availability.

## Focus behavior

`sysc-shell` matches sender PID ancestry against its cached Niri window state when it receives a
notification. It stores only an ephemeral notification-ID-to-window-ID target. If several windows match,
the shell marks the target ambiguous and does not focus a guessed window. Window IDs never survive a
shell restart.

## State and failure

Version one keeps bounded active state and history in memory. Restart loses both. Persistence needs a
separate format, migration, privacy, and retention design.

Loss of the D-Bus name terminates the service with an error. Shell disconnect preserves notification
state. Malformed presentation messages close that peer without disturbing D-Bus clients.

## Proof

Unit tests exercise the exported D-Bus methods rather than only internal helpers. Race tests cover expiry,
replacement, close, action, and reconnect ordering. Integration tests use a private session bus, then run
common Linux applications against the service. A live Niri gate proves popup action and conservative
focus behavior through `sysc-shell`.
