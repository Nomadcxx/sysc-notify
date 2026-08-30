# sysc-notify Persistence Addendum

Date: 2026-08-30
Status: Locked for Milestone 5 (owner D2). Implementation lives in `sysc-notify`, not `sysc-shell`.
Supersedes: `2026-08-27-sysc-notify-design.md` § State and failure, sentence "Version one keeps bounded active state and history in memory. Restart loses both."

Copy of the addendum also lives in the sysc-shell M5 worktree:
`docs/plans/2026-08-30-sysc-notify-persistence-design.md`.

## Decision

Disk history is owned by **sysc-notify**. The shell never writes a history file. The presentation snapshot grows a `history` array beside `active`. Service restart loads history from disk; active notifications start empty (the D-Bus name was lost).

## Files

| Path | Mode | Contents |
|---|---|---|
| `$XDG_STATE_HOME/sysc-notify/history.json` | 0600 | Versioned JSON object with history entries, newest last |
| `$XDG_STATE_HOME/sysc-notify/images/<sha256>.png` | 0600 | Downscaled PNG, at most 96 px on the long side |

`XDG_STATE_HOME` defaults to `$HOME/.local/state`. Create the directory 0700.

## Entry schema

```json
{
  "version": 1,
  "entries": [{
  "id": 42,
  "seen": false,
  "app_name": "Firefox",
  "app_icon": "firefox",
  "desktop_entry": "firefox",
  "summary": "Download complete",
  "body": "report.pdf",
  "urgency": 1,
  "category": "",
  "timestamp": "2026-08-30T10:15:00Z",
  "image": "images/<sha256>.png"
  }]
}
```

No actions on disk. History cards cannot invoke after the live notification is gone. Active notifications keep actions in the `active[]` snapshot only.

Skip: `transient` true and `x-sysc-private` true. Canonical and GNOME synchronization hints do not
imply privacy. DND is a shell presentation flag and does not affect this file.

## Bounds

- Cap **100** entries. Evict oldest when inserting past the cap; delete orphaned image files.
- Retention **7 days** default. A 60 s timer drops entries older than retention.
- Do not re-copy unbounded image-data onto disk: decode, downscale, PNG-encode, hash, write once.
- Atomic replace: write a unique temporary file, sync it, rename it, then sync the directory.
- Quarantine corrupt and unknown future schemas instead of overwriting them.
- Remove orphaned sidecars only after a valid committed history object supplies the reference set.

## Snapshot

On shell connect, send:

```json
{"type":"snapshot","active":[...],"history":[...]}
```

Later events: `added` / `replaced` / `closed` for active; `history-added` / `history-removed` /
`history-seen` / `history-cleared` for disk. Fall-behind still drops the connection. Reconnect gets a
fresh snapshot including history.

`history.clear` removes closed history. `history.mark-seen` persists seen state. `active.dismiss-all`
dismisses active records and does not clear history.

## Privacy

0600 files, 0700 directory. Do not persist sender PIDs.

## Capabilities

Once this path ships, advertise `persistence`. Until then, do not.

## Tests

- Restart the service: `history.json` reloads; `active` is empty.
- Cap 100: the 101st insert drops the oldest and its PNG.
- Transient never appears on disk.
- Malformed or future-schema JSON on load: quarantine it, log, start empty, and never overwrite it.
- Seen state survives restart and drives the shell's unread count.
- Shell reconnect after persist: snapshot `history` matches disk.
