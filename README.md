# sysc-notify

`sysc-notify` is a Go notification service for Linux desktops. It owns the
`org.freedesktop.Notifications` D-Bus interface and sends renderer-neutral notification state to
[`sysc-shell`](https://github.com/Nomadcxx/sysc-shell).

The repository is in the design stage. It contains no production daemon yet.

## Responsibilities

The first releases will provide:

- freedesktop notification protocol 1.3 behavior;
- exact replacement IDs, expiry, actions, close reasons, and required signals;
- bounded text, actions, hints, image data, active notifications, and history;
- sender process metadata for conservative Niri window matching in `sysc-shell`;
- a versioned Unix-socket protocol for shell presentation and user actions;
- snapshot recovery when the shell reconnects.

`sysc-shell` owns popup surfaces, theme, layout, input, and Niri focus. `sysc-notify` does not import
Wayland, create windows, or focus compositor windows. It continues accepting a bounded number of
notifications while the shell is unavailable.

Initial history is memory-only. Persistent history, cross-compositor focus integration, and a second UI
frontend require later design gates.

## Development gates

1. Pin the freedesktop 1.3 contract and its trust-boundary limits in tests.
2. Implement the D-Bus service, lifecycle, replacement, expiry, actions, and close signals.
3. Add bounded shell IPC with reconnect snapshots and backpressure.
4. Integrate shell-owned popup rendering and unambiguous notification-to-window focus.
5. Qualify common applications and shell restart behavior before `v0.1.0`.

See the [design](docs/plans/2026-08-27-sysc-notify-design.md) and [roadmap](docs/roadmap.md).
Package directories will arrive with their first tested behavior.

## Licence

`sysc-notify` uses the [BSD 3-Clause License](LICENSE).
