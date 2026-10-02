<p align="center">
  <img src="assets/header.png" width="920" alt="sysc-notify" />
</p>

A notification daemon for Linux, written in Go. It owns `org.freedesktop.Notifications` on the
session bus and hands every notification to [sysc-shell](https://github.com/Nomadcxx/sysc-shell),
which draws the popups, the notification centre and the history.

The daemon has no windows of its own. It keeps the state: what is active, when it expires, and what
went into history. When the shell restarts, it picks up exactly where it left off.

## Features

- **Freedesktop spec 1.3**: replacement IDs, expiry, actions, close reasons and the
  `NotificationClosed` / `ActionInvoked` signals
- **Inline replies**: the KDE `x-kde-reply-placeholder-text` hint and the `NotificationReplied`
  signal, so chat apps can take a reply from the popup
- **Images**: `image-data`, the older `image_data` / `icon_data` aliases, and `image-path` as a
  file path, a `file://` URI or a theme icon name. Large images are scaled to 512 px, and a
  malformed image is dropped rather than the notification
- **Progress and urgency**: the `value` hint for progress bars. When more than 128 notifications are
  active at once, the oldest non-critical one makes room
- **History**: up to 100 closed notifications, kept for 7 days and saved across restarts. Transient
  notifications, and ones marked `x-sysc-private`, are never written to disk
- **Survives the shell**: notifications keep arriving while the shell is down or restarting, and it
  gets the full state back when it reconnects
- **Sender tracking**: records which process sent a notification, so clicking it can focus the
  sender's Niri window. If more than one window matches, the shell focuses none rather than guess

## Installation

**Requires:** Go 1.26+ and a D-Bus session bus.

### From source

```bash
git clone https://github.com/Nomadcxx/sysc-notify
cd sysc-notify
go build -o ~/.local/bin/sysc-notify ./cmd/sysc-notify
```

### Via Go

```bash
GOBIN="$HOME/.local/bin" go install github.com/Nomadcxx/sysc-notify/cmd/sysc-notify@latest
```

### Run it as a user service

```bash
install -Dm644 contrib/sysc-notify.service ~/.config/systemd/user/sysc-notify.service
systemctl --user daemon-reload
systemctl --user enable --now sysc-notify.service
```

> Only one program can own the notification bus name. Stop mako, dunst, swaync or whatever you
> ran before, or sysc-notify exits at startup and systemd keeps restarting it. A daemon that is
> D-Bus activatable (xfce4-notifyd ships that way) can also grab the name if a notification arrives
> before sysc-notify is up.

Check which process owns the name:

```bash
busctl --user status org.freedesktop.Notifications | grep -E '^(PID|Comm)='
```

## Usage

Anything that sends desktop notifications works as-is:

```bash
notify-send "Build finished" "All tests passed"
notify-send -i dialog-information -u critical "Disk almost full"
```

sysc-shell shows them as popups and keeps closed ones in its notification history. The daemon takes
no flags. It needs `XDG_RUNTIME_DIR`, and it stops cleanly on `SIGINT` or `SIGTERM`.

| What | Where |
|---|---|
| Shell socket | `$XDG_RUNTIME_DIR/sysc-notify/presenter.v1.sock` |
| History | `$XDG_STATE_HOME/sysc-notify/history.json` (`~/.local/state/sysc-notify/` when unset) |
| Logs | `journalctl --user -u sysc-notify` |

## Writing a presenter

sysc-shell is one client of the presenter socket, not the only possible one. The wire types live in
the public `protocol` package:

```bash
go get github.com/Nomadcxx/sysc-notify/protocol
```

A presenter connects, sends a hello, and gets a snapshot of every active notification and the
history, then a stream of changes. It reports what it is showing so expiry pauses while you hover a
popup. A second presenter that connects replaces the first. The types in `protocol/` are the
reference.

## Development

```bash
go vet ./...
go test -race -count=1 ./...
dbus-run-session -- go test -race -count=1 ./tests/integration/
```

## License

BSD-3-Clause

---

<a href="https://github.com/Nomadcxx"><img src="https://raw.githubusercontent.com/Nomadcxx/Nomadcxx/main/assets/rama-mark.svg" height="22" alt="RAMA"></a> — terminal-native tooling for the linux desktop.
[More projects →](https://github.com/Nomadcxx) · [Sponsor](https://github.com/sponsors/Nomadcxx) ❤️
