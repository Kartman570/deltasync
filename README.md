# DeltaSync

A small Python/PostgreSQL synchronizer with a generated HTTP source, paginated
change cursors, updates, tombstones, durable checkpoints and snapshot repair.
No accounts, external services, datasets or credentials are needed.

## Quickstart

Requires Python 3.11+ and PostgreSQL 14+ server tools (`initdb` and `pg_ctl`) on
PATH. Validated on macOS with Python 3.14.5 and PostgreSQL 17.12;
the minimum versions and Linux have not been exercised here.
Run as a normal user, not root. macOS and Linux are supported.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m deltasync demo
.venv/bin/python -m deltasync test
.venv/bin/ruff check .
```

Run these commands from the repository root. The demo and test commands create
isolated disposable PostgreSQL clusters in private temporary directories. They
use Unix sockets, disable TCP listening and remove their clusters on completion.
Local trust authentication applies only to these generated-data clusters.
On Homebrew, use `brew install postgresql@17` and then
`export PATH="$(brew --prefix postgresql@17)/bin:$PATH"`; `libpq` alone
contains client tools and is insufficient. Do not start a system service.
No existing PostgreSQL service is needed or modified. A PostgreSQL installation
must already exist; these commands do not install one.

The demo starts a real loopback HTTP server and launches the CLI worker as a
separate process. After page one commits, the worker pauses after writing event
10 inside page two's transaction. A separate connection verifies that only the
first seven records and cursor 7 are visible. The demo sends **SIGKILL**, restarts
the CLI, then queries PostgreSQL to compare every live record with the source.
It checks all 73 unique change events, 13 tombstones and 27 live records, reruns
sync to check idempotence, injects missing/corrupt/orphaned rows, and repairs them
with a paginated full snapshot. A failed check exits nonzero.

Expected output includes:

```json
{
  "result": "PASS",
  "killed_signal": "SIGKILL",
  "checkpoint_after_kill": 7,
  "checkpoint_after_restart": 73,
  "applied_events": 73,
  "distinct_events": 73,
  "live_records": 27,
  "tombstones_applied": 13,
  "repeated_sync": "PASS",
  "snapshot_repair": "PASS"
}
```

## Manual workflow

For a dedicated empty local database that you provision separately:

```sh
export DELTASYNC_DSN='host=/path/to/local/socket dbname=deltasync user=deltasync'
.venv/bin/python -m deltasync init
.venv/bin/python -m deltasync serve
```

`serve` prints its randomly allocated loopback URL. In a second terminal, set the
same DSN and substitute that URL:

```sh
.venv/bin/python -m deltasync sync --api http://127.0.0.1:PORT --page-size 7
.venv/bin/python -m deltasync status
.venv/bin/python -m deltasync reconcile --api http://127.0.0.1:PORT
```

`init` creates three tables; use a dedicated database, because table names are
not namespaced. `status` prints live records. `reconcile` intentionally replaces
all synchronized records with the source snapshot. It keeps the event ledger;
snapshot bootstrapping does not claim to have individually processed old events.

## Correctness model

* A source has one ordered, contiguous, immutable change log. Each event has a
  positive sequence number, stable record ID and JSON object value, or null for
  deletion. A cursor is the number of consumed changes. GET `/changes` accepts
  `cursor` and `limit`, returning `items`, `next`, `done` and `watermark`.
* Each page locks the single checkpoint row and checks that it still matches the
  cursor used to fetch the page. Competing workers discard stale pages and retry.
  The event ledger, version-guarded record upserts and checkpoint advance commit
  together. An interrupted transaction rolls back; a committed page survives an
  immediate restart. PostgreSQL's normal durable commit settings remain enabled.
* Record IDs and event sequences are primary keys. Retained tombstones prevent
  old updates from resurrecting deleted records. Delivery is at least once;
  database application is idempotent. There are no external side effects.
* GET `/snapshot` is an immutable, consistent live-record snapshot with one
  watermark and offset pagination. Reconciliation locks the checkpoint, stages
  every page in a temporary table, validates the watermark, then atomically
  replaces records and advances the cursor. Missing rows are deleted only after
  the complete snapshot succeeds. Errors roll back all changes. Snapshots older
  than the checkpoint are rejected. Incremental sync resumes after the watermark.

## Scope and limits

The simulated API is immutable for the lifetime of its server. A real provider
must offer a consistent snapshot token/watermark and stable change log; checking
watermark equality alone cannot make an inconsistent provider safe. Expired
cursors require explicit reconciliation. HTTP failures leave checkpoints intact;
restart the command to retry. Requests have a five-second timeout.

This version handles one source per database, uses bounded change pages, and
holds a transaction/row lock across snapshot downloads. Large snapshots need a
persistent staging/job design to shorten locks and resume downloads. Event
history and tombstones have no retention policy. No authentication, scheduling,
production deployment, real provider adapter or automatic retry/backoff is
included. The crash demo verifies worker-process failure, not host power loss
or PostgreSQL failover. Windows is not supported by the SIGKILL/Unix-socket demo.
