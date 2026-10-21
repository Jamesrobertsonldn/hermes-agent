"""Native Kanban custody export includes registered and archived provenance stores."""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

from hermes_cli import kanban_custody as custody


SCHEMA = """
CREATE TABLE tasks (
    id TEXT PRIMARY KEY, status TEXT, created_by TEXT, created_at INTEGER, completed_at INTEGER,
    tenant TEXT, workspace_kind TEXT, workspace_path TEXT, branch_name TEXT, project_id TEXT,
    idempotency_key TEXT, claim_lock TEXT, claim_expires INTEGER, current_run_id INTEGER,
    worker_pid INTEGER, worker_started_at TEXT, body TEXT
);
CREATE TABLE task_runs (
    id INTEGER PRIMARY KEY, task_id TEXT, profile TEXT, step_key TEXT, status TEXT,
    claim_lock TEXT, claim_expires INTEGER, worker_pid INTEGER, worker_started_at TEXT,
    started_at INTEGER, ended_at INTEGER, outcome TEXT, summary TEXT
);
CREATE TABLE task_events (
    id INTEGER PRIMARY KEY, task_id TEXT, run_id INTEGER, kind TEXT, payload TEXT, created_at INTEGER
);
CREATE TABLE task_comments (
    id INTEGER PRIMARY KEY, task_id TEXT, author TEXT, body TEXT, created_at INTEGER
);
"""


class CustodyExportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="native custody ")
        self.addCleanup(self.temporary.cleanup)
        self.home = Path(self.temporary.name) / "hermes home"
        self.home.mkdir()
        self.path = self.home / "kanban.db"
        self.writer = self._open_store(self.path)
        self.addCleanup(self.writer.close)

    def _open_store(self, path: Path) -> sqlite3.Connection:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.executescript(SCHEMA)
        return conn

    def _seed(self, conn: sqlite3.Connection, task_id="t_12345678", status="done"):
        row_id = int(task_id[-1], 16) + 1
        conn.execute(
            "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (task_id, status, "issue-triage", 10, 20, "tenant-a", "worktree",
             "/source/.worktrees/owned", "wt/owned", "project-a", "source-issue-key",
             None, None, None, None, None, "PRIVATE BODY"),
        )
        conn.execute(
            "INSERT INTO task_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (row_id, task_id, "coder", None, "done", "host:100", None, 4321,
             "boot-id|process-start", 1, 2, "completed", "PRIVATE SUMMARY"),
        )
        conn.execute(
            "INSERT INTO task_events VALUES (?,?,?,?,?,?)",
            (row_id, task_id, row_id, "created", json.dumps({"source": "issue-triage", "actor": "receiver",
                                                    "reason": "PRIVATE REASON",
                                                    "message": "PRIVATE EVENT TEXT"}), 10),
        )
        conn.execute(
            "INSERT INTO task_comments VALUES (?,?,?,?,?)",
            (row_id, task_id, "coder", "Published https://github.com/owner/repo/pull/42\\nPRIVATE COMMENT", 11),
        )

    def test_includes_all_registered_archived_and_pinned_stores(self):
        self._seed(self.writer, status="archived")
        registered = self.home / "kanban" / "boards" / "other"
        registered_db = self._open_store(registered / "kanban.db")
        self.addCleanup(registered_db.close)
        self._seed(registered_db, "t_other001", "running")
        archived = self.home / "kanban" / "boards" / "_archived" / "retired-20261020"
        (self.home / "kanban" / "boards" / "other" / "board.json").write_text('{"archived": true}')
        archived_db = self._open_store(archived / "kanban.db")
        self.addCleanup(archived_db.close)
        self._seed(archived_db, "t_old0001", "done")
        result = custody.export_custody(self.home, str(self.path))
        self.assertEqual(result["scope"], "native_kanban_registered_and_archived_stores")
        self.assertTrue(result["store_scan_complete"])
        self.assertFalse(result["proves_subject_absence"])
        self.assertFalse(result["proves_non_kanban_writer_absence"])
        self.assertFalse(result["cross_board_snapshot_atomic"])
        self.assertEqual({board["store_kind"] for board in result["boards"]}, {"registered", "archived"})
        self.assertTrue(next(board for board in result["boards"] if board["board_id"] == "other")["board_archived"])
        self.assertTrue(next(board for board in result["boards"] if board["store_kind"] == "archived")["board_archived"])
        self.assertEqual(sum(len(board["tasks"]) for board in result["boards"]), 3)
        default = next(board for board in result["boards"] if board["board_id"] == "default")
        self.assertEqual(default["soft_tombstones"], ["t_12345678"])
        self.assertEqual(default["runs"][0]["worker_pid"], 4321)
        self.assertEqual(default["producer_events"][0]["provenance"],
                         {"source": "issue-triage", "actor": "receiver"})
        self.assertEqual(default["pr_markers"][0]["url"], "https://github.com/owner/repo/pull/42")
        self.assertNotIn("PRIVATE", json.dumps(result))

    def test_retains_orphan_pr_marker_as_deleted_task_custody(self):
        self.writer.execute(
            "INSERT INTO task_comments VALUES (?,?,?,?,?)",
            (9, "t_deleted1", "operator", "https://github.com/owner/repo/pull/99", 99),
        )
        default = custody.export_custody(self.home)["boards"][0]
        self.assertEqual(default["pr_markers"][0]["task_id"], "t_deleted1")
        self.assertFalse(default["pr_markers"][0]["task_present"])
        self.assertEqual(default["hard_delete_history"], "not_recorded_by_native_kanban_schema")

    def test_reads_uncheckpointed_wal_and_connection_rejects_mutation(self):
        self._seed(self.writer)
        before = {path: path.read_bytes() for path in (self.path, Path(str(self.path) + "-wal"))}
        original = custody._rows

        def attempt_mutation(conn, *args, **kwargs):
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("DELETE FROM tasks")
            return original(conn, *args, **kwargs)

        with mock.patch.object(custody, "_rows", side_effect=attempt_mutation):
            result = custody.export_custody(self.home)
        self.assertEqual(result["boards"][0]["runs"][0]["worker_pid"], 4321)
        self.assertEqual(before, {path: path.read_bytes() for path in before})

    def test_concurrent_commit_cannot_mix_per_board_snapshot(self):
        self._seed(self.writer)
        original = custody._rows

        def commit_between_tables(conn, table, *args, **kwargs):
            rows = original(conn, table, *args, **kwargs)
            if table == "tasks":
                self.writer.execute("BEGIN")
                self.writer.execute("UPDATE tasks SET status='running',worker_pid=9876")
                self.writer.execute("UPDATE task_runs SET worker_pid=9876")
                self.writer.execute("COMMIT")
            return rows

        with mock.patch.object(custody, "_rows", side_effect=commit_between_tables):
            result = custody.export_custody(self.home)
        default = result["boards"][0]
        self.assertEqual(default["tasks"][0]["status"], "done")
        self.assertEqual(default["runs"][0]["worker_pid"], 4321)
        self.assertEqual(custody.export_custody(self.home)["boards"][0]["runs"][0]["worker_pid"], 9876)

    def test_missing_default_store_or_schema_fails_without_creation_or_migration(self):
        missing_home = self.home / "missing"
        with self.assertRaisesRegex(RuntimeError, "no absence proof"):
            custody.export_custody(missing_home)
        self.assertFalse(missing_home.exists())
        old = self.home / "old" / "kanban.db"
        old.parent.mkdir()
        with sqlite3.connect(old) as conn:
            conn.execute("CREATE TABLE tasks (id TEXT)")
        before = old.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "complete native tasks custody"):
            custody.export_custody(old.parent)
        self.assertEqual(before, old.read_bytes())

    def test_discovered_board_without_database_fails_instead_of_skipping(self):
        (self.home / "kanban" / "boards" / "registered-only").mkdir(parents=True)
        (self.home / "kanban" / "boards" / "registered-only" / "board.json").write_text("{}")
        with self.assertRaisesRegex(RuntimeError, "missing a discovered board database"):
            custody.export_custody(self.home)

    def test_limit_fails_instead_of_returning_truncated_evidence(self):
        self._seed(self.writer, "t_12345678")
        self._seed(self.writer, "t_87654321")
        with mock.patch.object(custody, "MAX_TOTAL_ROWS", 1):
            with self.assertRaisesRegex(RuntimeError, "row limit"):
                custody.export_custody(self.home)

    def _command(self, database: Path):
        from hermes_cli import kanban

        self.assertTrue(Path(kanban.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[2]))
        parser = argparse.ArgumentParser()
        kanban.build_parser(parser.add_subparsers(dest="command"))
        args = parser.parse_args(["kanban", "--board", "default", "custody", "--json"])
        output, errors = io.StringIO(), io.StringIO()
        with mock.patch.dict(os.environ, {"HERMES_KANBAN_HOME": str(database.parent),
                                         "HERMES_KANBAN_DB": str(database)}):
            with mock.patch.object(kanban.kb, "init_db") as initialize:
                with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
                    code = kanban.kanban_command(args)
                initialize.assert_not_called()
        return code, output.getvalue(), errors.getvalue()

    def test_cli_missing_database_never_auto_initializes(self):
        missing = self.home / "missing" / "kanban.db"
        code, output, errors = self._command(missing)
        self.assertNotEqual(code, 0)
        self.assertEqual(output, "")
        self.assertIn("no absence proof", errors)
        self.assertFalse(missing.parent.exists())

    def test_cli_exports_valid_wal_snapshot_without_database_changes(self):
        self._seed(self.writer)
        before = {path: path.read_bytes() for path in (self.path, Path(str(self.path) + "-wal"))}
        code, output, errors = self._command(self.path)
        self.assertEqual((code, errors), (0, ""))
        result = json.loads(output)
        self.assertEqual(result["boards"][0]["runs"][0]["worker_started_at"], "boot-id|process-start")
        self.assertFalse(result["proves_subject_absence"])
        self.assertEqual(before, {path: path.read_bytes() for path in before})

    def test_worker_context_cannot_export_other_boards(self):
        from argparse import Namespace

        errors = io.StringIO()
        with mock.patch.dict(os.environ, {"HERMES_KANBAN_TASK": "t_worker01"}):
            with mock.patch.object(custody, "export_custody", side_effect=AssertionError("must not read")):
                with contextlib.redirect_stderr(errors):
                    code = custody.custody_command(Namespace())
        self.assertNotEqual(code, 0)
        self.assertIn("operator-only", errors.getvalue())
