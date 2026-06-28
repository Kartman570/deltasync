from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

from .local import database, worker_environment
from .source import events, serve, snapshot
from .store import connect, cursor, initialize, state


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def run():
    with database() as dsn, serve() as url, tempfile.TemporaryDirectory(prefix="deltasync-demo-") as tmp:
        env = worker_environment(dsn)
        command = [sys.executable, "-m", "deltasync", "sync", "--api", url]
        ready = Path(tmp) / "ready"
        with connect(dsn) as conn:
            initialize(conn)
            worker = subprocess.Popen(command + ["--pause-at", "10", "--ready-file", str(ready)],
                                      env=env, stdout=subprocess.DEVNULL)
            try:
                deadline = time.monotonic() + 15
                while not ready.exists():
                    require(worker.poll() is None, "worker exited before crash barrier")
                    require(time.monotonic() < deadline, "worker did not reach crash barrier")
                    time.sleep(0.02)
                require(cursor(conn) == 7, "partial page checkpoint leaked")
                require(len(state(conn)) == 7, "uncommitted records became visible")
                worker.kill()
                require(worker.wait(timeout=10) == -signal.SIGKILL, "worker was not killed")
            finally:
                if worker.poll() is None:
                    worker.kill()
                    worker.wait(timeout=10)
            require(cursor(conn) == 7, "checkpoint changed after crash")
            subprocess.run(command, env=env, check=True, capture_output=True, timeout=30)
            expected = {item["id"]: item["value"] for item in snapshot(events())}
            require(state(conn) == expected, "recovered state differs from source")
            count, unique = conn.execute("SELECT count(*), count(DISTINCT seq) FROM applied_events").fetchone()
            require(count == unique == len(events()), "missing or duplicate applied events")
            require(cursor(conn) == len(events()), "incomplete checkpoint")
            subprocess.run(command, env=env, check=True, capture_output=True, timeout=30)
            require(conn.execute("SELECT count(*) FROM applied_events").fetchone()[0] == count,
                    "repeated sync duplicated events")
            require(state(conn) == expected, "repeated sync changed state")
            conn.execute("DELETE FROM records WHERE id = '1'")
            conn.execute("UPDATE records SET value = '{}' WHERE id = '2'")
            conn.execute("INSERT INTO records VALUES ('orphan', 1, '{}')")
            subprocess.run([sys.executable, "-m", "deltasync", "reconcile", "--api", url],
                           env=env, check=True, capture_output=True, timeout=30)
            require(state(conn) == expected, "reconciliation did not repair drift")
            subprocess.run(command, env=env, check=True, capture_output=True, timeout=30)
            require(state(conn) == expected, "sync after reconciliation changed state")
            return {"result": "PASS", "killed_signal": "SIGKILL", "checkpoint_after_kill": 7,
                    "checkpoint_after_restart": cursor(conn), "applied_events": count,
                    "distinct_events": unique, "live_records": len(expected),
                    "tombstones_applied": sum(item["value"] is None for item in events()),
                    "repeated_sync": "PASS", "snapshot_repair": "PASS"}
