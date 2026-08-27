# sysc-notify Roadmap

Date: 2026-08-27

## M0: Protocol contract

- pin notification specification 1.3 behavior;
- define ingress limits and honest capabilities;
- define the first shell IPC messages and reconnect snapshot.

Gate: contract tests cover replacement IDs, close reasons, action signals, expiry, and malformed images.

## M1: Headless notification service

- acquire `org.freedesktop.Notifications`;
- implement methods, signals, active state, memory history, and expiry;
- collect sender PID metadata without D-Bus monitoring;
- run against a private session bus.

Gate: applications can send, replace, close, and act on notifications without a presentation client.

## M2: Shell transport

- add the private Unix socket, peer validation, bounds, and version handshake;
- send a current snapshot on connect and ordered changes afterward;
- accept action and dismiss messages from the shell;
- disconnect slow or malformed peers and recover by snapshot.

Gate: repeated shell disconnect and reconnect loses no active notification while the service remains up.

## M3: sysc-shell presentation

- render notifications on shell-owned layer surfaces;
- support actions, images, progress, urgency, timeout policy, and dismiss;
- resolve sender lineage against cached Niri windows;
- focus only one unambiguous window target.

Gate: common applications display correctly on Niri and a shell restart restores active popups.

## M4: First release

- qualify limits, race behavior, service-manager startup, and application compatibility;
- document unsupported hints and capabilities;
- tag `v0.1.0` with the matching shell protocol version.

Persistent history and activation tokens remain later gates.
