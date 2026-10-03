# sysc-notify presenter & history audit report

- **Date:** 2026-10-03
- **Target:** `github.com/Nomadcxx/sysc-notify`
- **Revision audited:** `43d9d03ea3981a376a248c881e98d51bf8c43509` (`main`)
- **Trigger:** intermittent shell crash under rapid-succession notification bursts (mainly the `ai-usage` plugin)
- **Scope:** presenter delivery path, owner goroutine, history persistence, FDO signal emission. The shell crash itself is diagnosed in the companion `sysc-shell` report.

## 0. Evidence → Finding → Path

| Evidence | Finding | Path |
|---|---|---|
| `internal/presenter/connection.go:88` — `enqueueDelta` calls `c.fail()` when `reserve` or the `select` default fails | **F1** A bounded presenter queue that fills under a burst tears the whole connection down, forcing the shell to reconnect | `internal/presenter/connection.go:127` `reserve`; `protocol/types.go:25` `MaxPresenterQueueMessages=256` |
| `internal/state/owner.go:510,542,556,570`; `internal/state/expiry.go:98`; `internal/state/owner.go:664` | **F2** History disk I/O (`Write`+`Sync`+`Rename`, ~21 ms per image entry) runs on the owner goroutine, stalling every command behind it | `internal/history/store.go` `commit` |
| `internal/presenter/server.go:127` (`main`) | **F3** Handshake deltas are sequence-filtered: the snapshot covers earlier events, and later deltas are queued | `internal/presenter/connection.go:39` `prepare` |
| Captured error: shell-side Wayland `create_region` protocol failure | **F4** The observed crash is a shell protocol failure, not a `sysc-notify` panic | `cmd/sysc-notify/main.go:18` |
| `internal/fdo/server.go` `Publish` `default:` on `emitQueue` | **F5** FDO signal emission drops on overflow by design; informational, not a fault | `internal/fdo/server.go` |

## 1. Method

- Full read of all production packages: `cmd/sysc-notify`, `internal/app`, `internal/state`, `internal/presenter`, `internal/fdo`, `internal/notify`, `internal/history`, `internal/sender`, `protocol`.
- Static checks: `go vet ./...` and `go build ./...` clean.
- `go test -race -count=1 ./...` — all packages pass; no data races reported.
- A private test D-Bus (via `internal/dbustest`) plus a live daemon was driven with a burst harness to validate the invariants below against real sockets and a real presenter client. The harness was a scratch artifact and is not committed; its measurements are reproduced in §3.

## 2. Delivery chain

```
ai-usage plugin ──D-Bus Notify──▶ FDO service ──▶ owner goroutine ──▶ Sink.Publish ──▶ presenter socket ──▶ shell notifyclient
                                                      │
                                                      └─▶ history.Store (disk)
```

The shell publishes each plugin toast under a **unique** producer key (`sysc-shell:plugin-toast:<seq>`), so a burst is a stream of distinct records, not replacements. Each record is `Transient=true`, which means the producer path performs **no** history I/O — F2 is only reachable for resident/actionable notifications and history commands.

## 3. Measurements (burst harness)

| Scenario | Result |
|---|---|
| Producer path, 4 000 publishes | 0.74 ms total, contiguous sequences, no teardown |
| D-Bus replace loop, 1 000 replacements | 346 ms |
| D-Bus + images + history, 200 notifications | 4.3 s (~21 ms each) |
| Slow consumer past `MaxPresenterQueueMessages` (256) | connection **torn down** by design, clean snapshot recovery on reconnect |
| Owner `Notify` latency, 1 200 calls | max **81.6 ms** |
| `MaxActiveNotifications=128` | enforced |

## 4. Findings

### F1 — bounded-queue teardown on a slow consumer (Medium, proven)

`connection.enqueueDelta` fails the connection when either the message/byte reserve or the non-blocking queue send fails (`internal/presenter/connection.go:88`). The queue is bounded by `MaxPresenterQueueMessages = 256` and `MaxPresenterDecodedBytes = 32 MiB` (`protocol/types.go:25-26`).

A reader that stalls (the shell renders on the same client that reads the socket) stops draining the queue. Once 256 frames back up, the presenter drops the connection. The daemon stays healthy — the shell reconnects and rebuilds from a fresh snapshot — but the churn is real, and each reconnect re-runs the handshake. This is the contributor that makes a rapid burst visible to the shell as a disconnect.

**Fix (companion hardening PR):** replace the overflow teardown with a **mid-stream snapshot resync**. On overflow the owner builds a fresh snapshot (serialized with deltas on the owner goroutine, so its sequence is a clean baseline), the writer drains the queued stale deltas and then writes the snapshot, and subsequent deltas continue contiguously from the new baseline. No reconnect, no generation churn. The shell client already accepts a mid-stream `KindSnapshot` (it resets its sequence baseline without requiring a new generation).

### F2 — history disk I/O on the owner goroutine (Low–Medium, proven)

Every mutator and the periodic sweep run `history.Store.commit` — a JSON marshal, temp file, `Chmod(0600)`, `Write`, `Sync`, `Rename`, directory sync, and image-sidecar writes — synchronously on the **owner goroutine**. The measured worst-case `Notify` latency was 81.6 ms; image entries cost ~21 ms each. The owner goroutine is single-threaded, so every command behind a commit waits.

**Fix (companion hardening PR):** move persistence onto a background worker with a `Flush()`/`Close()` sync point. In-memory entries are authoritative and updated synchronously; mutations coalesce into a single commit. Deltas and snapshots never wait on disk. Persistence becomes *eventual*: a failed write is logged and surfaced via `Flush`/`Close`, and the in-memory entry is retained.

### F3 — presenter handshake drop window (Low)

While a connection is in `s.preparing`, `Publish` calls `prepare` (`internal/presenter/connection.go:39`), which buffers deltas for that connection. Deltas published before the handshake snapshot are handled by `activate` (dropped if `sequence <= snapshot.Sequence`, otherwise queued). The window is bounded by the handshake timeout, and the fresh snapshot supersedes it. Informational.

### F4 - captured crash originates in the shell (Informational)

The captured error is a Wayland `create_region` protocol failure in the shell. `main` calls `os.Exit(1)` when `app.Run` returns an error. The audit found no explicit `panic` call in production code.

### F5 — FDO emit queue drops on overflow (Informational)

`internal/fdo/server.go` `Publish` drops signals when `emitQueue` is full. This is bounded, intentional backpressure, not a fault.

## 5. Verdict

The crash is **not** a `sysc-notify` panic. `sysc-notify` contributes only via **F1**: under a burst a slow presenter is torn down, which the shell experiences as a disconnect/resync-storm. The fatal shell error is a Wayland `create_region` protocol failure on the shell side. F1 and F2 are hardened in the accompanying PR.

## 6. Reproduction

```sh
cd sysc-notify
go test -race -count=1 ./...          # all packages pass, no races
go test -count=1 -v -run TestSlowPresenter ./tests/integration/
```

The integration test drives a live daemon with a stalled presenter, triggers `MaxPresenterQueueMessages` overflow through 400 D-Bus publishes, and asserts the presenter receives a resync snapshot **without** being dropped.

## 7. Follow-up

- Shell-side: the fatal Wayland `wl_compositor.create_region` error and the `sysc-wayland` v0.3.1 → `main` decoder-fix gap are covered in the companion `sysc-shell` report.
- If F1 resync churn is still observed, consider raising `MaxPresenterQueueMessages` in addition to the resync (the shell client buffers only 32 messages before its pump).
