# sysc-notify v0.1 release-candidate record

Date: 2026-09-01

## Candidate

`v0.1.0-rc.2` adds the service-owned lifetime data that the shell needs to draw notification
countdowns. The service keeps `v0.1.0-rc.1` unchanged as the rollback tag. Stable `v0.1.0` still waits
for the shell's live Tranche 5A qualification.

## Wire contract

- Protocol version: 1.1.
- Presenter socket: `$XDG_RUNTIME_DIR/sysc-notify/presenter.v1.sock`, directory `0700`, socket `0600`.
- Required presenter capabilities: `notification-state` and `presentation-lifetime`.
- Every active snapshot record and add or replace delta carries one matching `Lifetime` with duration,
  remaining milliseconds, and running state.
- A successful `presentation.renew` reply returns the current lifetime for every active record.
- Peers without `presentation-lifetime` fail the handshake before the service sends state.
- Frames remain four-byte big-endian length-prefixed JSON with a 16 MiB frame limit.
- The service permits 128 active notifications, 100 history entries, and 256 queued presenter messages
  bounded to 32 MiB decoded data.

Protocol fixture hashes:

```text
d3d6a6eb5e0e40456810fbf6ec661f329344387fd804fa5a0707d409f1e29e39  protocol/types_test.go
016fa1f257745dc2a2bbf60eea6fb0585f591f78c1acb5ed45d15b0ef8c6e8c9  protocol/frame_test.go
```

## Candidate gate

The release worktree ran these commands on 2026-09-01:

```text
test -z "$(gofmt -l .)"
go vet ./...
go test -race -count=1 ./...
dbus-run-session -- go test -race -count=1 ./tests/integration/
go test ./protocol -run '^$' -fuzz '^FuzzReadFrame$' -fuzztime=10s
go test ./protocol -run '^$' -fuzz '^FuzzDecodeStrict$' -fuzztime=10s
go list -m all
git diff --exit-code -- go.mod go.sum
```

All commands exited zero. The race run passed every package. The D-Bus integration gate passed in
1.623 seconds. The frame fuzzer executed 45,702 cases; the strict decoder executed 252,627. The module
graph contains `github.com/godbus/dbus/v5 v5.2.2` and `golang.org/x/sys v0.27.0`, with no `replace`
directive.

## Known limits

Lifetime values are samples from the service owner at snapshot, add or replace, and renewal reply time.
The shell interpolates a running countdown between samples and stops interpolation while `Running` is
false. A service restart still drops active records because losing the D-Bus name ends their ownership;
history remains durable.
