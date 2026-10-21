"""Read-only export of native Kanban producer and worker custody evidence."""
from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import re
import sqlite3


MAX_TOTAL_ROWS = 100_000
TASK_FIELDS = (
    "id", "status", "created_by", "created_at", "completed_at", "tenant",
    "workspace_kind", "workspace_path", "branch_name", "project_id",
    "idempotency_key", "claim_lock", "claim_expires", "current_run_id",
    "worker_pid", "worker_started_at",
)
RUN_FIELDS = (
    "id", "task_id", "profile", "step_key", "status", "claim_lock",
    "claim_expires", "worker_pid", "worker_started_at", "started_at",
    "ended_at", "outcome",
)
EVENT_FIELDS = ("id", "task_id", "run_id", "kind", "payload", "created_at")
COMMENT_FIELDS = ("id", "task_id", "author", "body", "created_at")
PRODUCER_PAYLOAD_FIELDS = frozenset({
    "actor", "assignee", "from", "new_status", "old_status", "source", "status",
})
PR_URL_RE = re.compile(
    r"https://[A-Za-z0-9.-]+(?::[0-9]{1,5})?/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pull/[0-9]+(?:/)?"
)


def _board_databases(home: Path, override: str | None) -> list[dict]:
    """Enumerate registered and archived board DBs without following symlinks."""
    sources: dict[str, dict] = {}
    root = home / "kanban" / "boards"

    def add(path: Path, board_id: str, store_kind: str, metadata_path: Path | None = None) -> None:
        if path.is_symlink():
            raise RuntimeError(f"custody export cannot follow a symlinked board database: {path}")
        if not path.exists():
            raise RuntimeError(f"custody export is missing a discovered board database: {path}")
        if not path.is_file():
            raise RuntimeError(f"custody export found a non-file board database: {path}")
        key = str(path.resolve(strict=True))
        if key in sources:
            if store_kind == "pinned_override":
                sources[key]["also_pinned_override"] = True
            return
        archived = True if store_kind == "archived" else False if store_kind == "registered" else None
        if metadata_path and metadata_path.is_symlink():
            raise RuntimeError(f"custody export cannot follow symlinked board metadata: {metadata_path}")
        if metadata_path and metadata_path.exists():
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8-sig"))
            except (OSError, json.JSONDecodeError):
                archived = True if store_kind == "archived" else None
            else:
                if isinstance(metadata, dict) and isinstance(metadata.get("archived", False), bool):
                    archived = archived or metadata.get("archived", False)
                else:
                    archived = True if store_kind == "archived" else None
        sources[key] = {
            "board_id": board_id, "store_kind": store_kind, "board_archived": archived,
            "database_path": str(path),
        }

    add(home / "kanban.db", "default", "registered", root / "default" / "board.json")
    if root.is_symlink():
        raise RuntimeError(f"custody export cannot follow a symlinked boards directory: {root}")
    if root.exists():
        if not root.is_dir():
            raise RuntimeError(f"custody export boards path is not a directory: {root}")
        for entry in sorted(root.iterdir(), key=lambda item: item.name):
            if entry.is_symlink():
                raise RuntimeError(f"custody export cannot follow a symlinked board entry: {entry}")
            if not entry.is_dir():
                continue
            if entry.name == "_archived":
                for archived in sorted(entry.iterdir(), key=lambda item: item.name):
                    if archived.is_symlink():
                        raise RuntimeError(f"custody export cannot follow a symlinked archived board: {archived}")
                    if archived.is_dir():
                        add(archived / "kanban.db", archived.name, "archived", archived / "board.json")
                continue
            if (entry / "board.json").exists() or (entry / "kanban.db").exists():
                add(entry / "kanban.db", entry.name, "registered", entry / "board.json")

    if override:
        path = Path(override).expanduser()
        add(path, "pinned_override", "pinned_override")
    return [sources[key] for key in sorted(sources)]


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _lossy_text(value: bytes) -> str:
    return value.decode("utf-8", errors="replace")


def _rows(
    conn: sqlite3.Connection, table: str, fields: tuple[str, ...], budget: list[int], where: str = "",
) -> list[dict]:
    available = _table_columns(conn, table)
    if not available:
        raise RuntimeError(f"custody export requires native {table} table")
    required = {
        "tasks": {"id", "status", "created_by", "created_at", "tenant", "workspace_kind",
                  "workspace_path", "branch_name", "project_id", "idempotency_key", "claim_lock",
                  "claim_expires", "current_run_id", "worker_pid", "worker_started_at"},
        "task_runs": {"id", "task_id", "profile", "step_key", "status", "claim_lock",
                      "claim_expires", "worker_pid", "worker_started_at", "started_at", "ended_at", "outcome"},
        "task_events": {"id", "task_id", "run_id", "kind", "payload", "created_at"},
        "task_comments": {"id", "task_id", "author", "body", "created_at"},
    }.get(table, set())
    if not required.issubset(available):
        raise RuntimeError(f"custody export requires complete native {table} custody columns")
    selected = [field for field in fields if field in available]
    order = "id"
    cursor = conn.execute(
        f"SELECT {','.join(selected)} FROM {table} {where} ORDER BY {order} LIMIT ?",
        (MAX_TOTAL_ROWS - budget[0] + 1,),
    )
    result = cursor.fetchall()
    if len(result) > MAX_TOTAL_ROWS - budget[0]:
        raise RuntimeError("custody export exceeds total row limit; reconcile native stores directly")
    budget[0] += len(result)
    return [dict(row) for row in result]


def _producer_payload(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    if not isinstance(value, dict):
        return {}
    return {
        key: item[:256] if isinstance(item, str) else item
        for key, item in value.items()
        if key in PRODUCER_PAYLOAD_FIELDS and isinstance(item, (str, int, float, bool, type(None)))
    }


def _read_board(source: dict, budget: list[int]) -> dict:
    try:
        path = Path(source["database_path"]).resolve(strict=True)
        with contextlib.closing(sqlite3.connect(
            path.as_uri() + "?mode=ro", uri=True, timeout=5, isolation_level=None,
        )) as conn:
            conn.text_factory = _lossy_text
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")
            schema_version = conn.execute("PRAGMA user_version").fetchone()[0]
            tasks = _rows(conn, "tasks", TASK_FIELDS, budget)
            runs = _rows(conn, "task_runs", RUN_FIELDS, budget)
            events = _rows(conn, "task_events", EVENT_FIELDS, budget)
            comments = _rows(conn, "task_comments", COMMENT_FIELDS, budget, "WHERE body LIKE '%/pull/%'")
            task_ids = {task["id"] for task in tasks}
            producer_events = [
                {"id": event["id"], "task_id": event["task_id"], "run_id": event.get("run_id"),
                 "kind": event["kind"], "created_at": event["created_at"],
                 "provenance": _producer_payload(event.get("payload"))}
                for event in events
            ]
            pr_markers = []
            for comment in comments:
                for url in sorted(set(PR_URL_RE.findall(comment.get("body") or ""))):
                    pr_markers.append({
                        "task_id": comment["task_id"], "comment_id": comment["id"],
                        "author": comment.get("author"), "created_at": comment.get("created_at"),
                        "task_present": comment["task_id"] in task_ids,
                        "url": url.rstrip("/"),
                    })
            return {
                **source,
                "schema_version": schema_version,
                "tasks": tasks,
                "runs": runs,
                "producer_events": producer_events,
                "pr_markers": pr_markers,
                "soft_tombstones": [task["id"] for task in tasks if task["status"] == "archived"],
                "hard_delete_history": "not_recorded_by_native_kanban_schema",
            }
    except (OSError, sqlite3.Error) as exc:
        raise RuntimeError(f"native Kanban custody unavailable for {source['database_path']}") from exc


def export_custody(home: Path, override: str | None = None) -> dict:
    """Export every discoverable registered/archived board from stable per-DB snapshots.

    This is evidence from native Kanban stores. It cannot account for hard-deleted tasks,
    repositories unknown to Kanban, non-Kanban writers, or writes after each DB snapshot.
    """
    try:
        sources = _board_databases(home.resolve(strict=True), override)
        budget = [0]
        boards = [_read_board(source, budget) for source in sources]
        return {
            "schema_version": 2,
            "scope": "native_kanban_registered_and_archived_stores",
            "store_scan_complete": True,
            "proves_subject_absence": False,
            "proves_non_kanban_writer_absence": False,
            "cross_board_snapshot_atomic": False,
            "boards": boards,
        }
    except (OSError, sqlite3.Error) as exc:
        raise RuntimeError("native Kanban custody unavailable; no absence proof") from exc
    except RuntimeError as exc:
        raise RuntimeError(f"{exc}; no absence proof") from exc


def custody_command(_args) -> int:
    from agent.delegation_context import is_delegated_child_context, is_delegated_child_process_context
    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_output import _err, _print_json

    if (os.environ.get("HERMES_KANBAN_TASK") or is_delegated_child_context()
            or is_delegated_child_process_context()):
        return _err("kanban custody: operator-only; worker contexts cannot export other boards")
    try:
        _print_json(export_custody(kb.kanban_home(), os.environ.get("HERMES_KANBAN_DB")))
    except RuntimeError as exc:
        return _err(f"kanban: {exc}")
    return 0
