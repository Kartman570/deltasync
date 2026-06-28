import os
import unittest
from unittest.mock import patch

from deltasync.demo import run
from deltasync.source import events, serve, snapshot
from deltasync.store import apply_page, connect, cursor, initialize, state
from deltasync.sync import reconcile, sync


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.conn = connect(os.environ["DELTASYNC_DSN"])
        initialize(self.conn)
        self.conn.execute("TRUNCATE records, applied_events")
        self.conn.execute("UPDATE checkpoint SET cursor = 0")

    def tearDown(self):
        self.conn.close()

    def test_incremental_updates_tombstones_and_replay(self):
        with serve() as url:
            self.assertEqual(sync(self.conn, url, 3), 73)
            self.assertEqual(state(self.conn), {x["id"]: x["value"] for x in snapshot(events())})
            self.assertEqual(sync(self.conn, url, 10), 73)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM applied_events").fetchone()[0], 73)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM records WHERE value IS NULL").fetchone()[0], 13)

    def test_page_failure_rolls_back_checkpoint_and_rows(self):
        def fail(item):
            raise RuntimeError("injected worker failure")
        page = {"items": events()[:7], "next": 7}
        with self.assertRaises(RuntimeError):
            apply_page(self.conn, 0, page, fail)
        self.assertEqual(cursor(self.conn), 0)
        self.assertEqual(state(self.conn), {})
        self.assertEqual(self.conn.execute("SELECT count(*) FROM applied_events").fetchone()[0], 0)
        self.assertTrue(apply_page(self.conn, 0, page))
        self.assertFalse(apply_page(self.conn, 0, page))

    def test_stale_worker_cannot_overwrite_checkpoint(self):
        page = {"items": events()[:7], "next": 7}
        with connect(os.environ["DELTASYNC_DSN"]) as other:
            self.assertEqual(cursor(other), 0)
            apply_page(self.conn, 0, page)
            self.assertFalse(apply_page(other, 0, page))
            self.assertEqual(cursor(other), 7)

    def test_invalid_order_rejected_before_writes(self):
        with self.assertRaises(ValueError):
            apply_page(self.conn, 0, {"items": [events()[1]], "next": 1})
        self.assertEqual(state(self.conn), {})

    def test_snapshot_repairs_drift_and_resumes_changes(self):
        with serve(events()[:40]) as url:
            reconcile(self.conn, url, 4)
        self.assertEqual(cursor(self.conn), 40)
        self.conn.execute("UPDATE records SET value = '{}' WHERE id = '1'")
        self.conn.execute("INSERT INTO records VALUES ('orphan', 1, '{}')")
        with serve() as url:
            sync(self.conn, url)
            reconcile(self.conn, url)
        self.assertEqual(state(self.conn), {x["id"]: x["value"] for x in snapshot(events())})

    def test_inconsistent_snapshot_rolls_back(self):
        apply_page(self.conn, 0, {"items": events()[:7], "next": 7})
        before = state(self.conn)
        pages = [{"items": events()[:1], "next": 1, "watermark": 73, "done": False},
                 {"items": [], "next": 1, "watermark": 74, "done": True}]
        with patch("deltasync.sync.fetch", side_effect=pages), self.assertRaises(ValueError):
            reconcile(self.conn, "unused")
        self.assertEqual(state(self.conn), before)
        self.assertEqual(cursor(self.conn), 7)

    def test_empty_snapshot_removes_stale_rows(self):
        self.conn.execute("INSERT INTO records VALUES ('orphan', 1, '{}')")
        with serve([]) as url:
            reconcile(self.conn, url)
        self.assertEqual(state(self.conn), {})

    def test_snapshot_cannot_rewind_checkpoint(self):
        apply_page(self.conn, 0, {"items": events()[:7], "next": 7})
        with serve([]) as url, self.assertRaises(ValueError):
            reconcile(self.conn, url)
        self.assertEqual(cursor(self.conn), 7)

    def test_process_kill_restart_demo(self):
        result = run()
        self.assertEqual(result["result"], "PASS")
        self.assertEqual(result["applied_events"], 73)
        self.assertEqual(result["live_records"], 27)
