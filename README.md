# sysc-notify

`sysc-notify` is a Go notification service for Linux desktops. It owns the
`org.freedesktop.Notifications` D-Bus interface and sends renderer-neutral notification state to
[`sysc-shell`](https://github.com/Nomadcxx/sysc-shell).

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

The service stores bounded public history under the XDG state directory. It does not persist private or
transient notifications, sender PIDs, actions, or reply data.

## Run

```sh
go build ./cmd/sysc-notify
./sysc-notify
```

The daemon requires a session bus and `XDG_RUNTIME_DIR`. It creates the presenter socket at
`$XDG_RUNTIME_DIR/sysc-notify/presenter.v1.sock`. History defaults to
`$XDG_STATE_HOME/sysc-notify/history.json`, or `~/.local/state/sysc-notify/history.json` when
`XDG_STATE_HOME` is unset. `SIGINT` and `SIGTERM` trigger a clean shutdown.

Run the automated gate with:

```sh
go vet ./...
go test -race -count=1 ./...
dbus-run-session -- go test -race -count=1 ./tests/integration/
```

See the [design](docs/plans/2026-08-27-sysc-notify-design.md) and [roadmap](docs/roadmap.md).

## Licence

`sysc-notify` uses the [BSD 3-Clause License](LICENSE).
