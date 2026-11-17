"""SQLite persistence for the harvester.

The published catalogue and the harvest watermark live in the same SQLite
database.  A harvest round writes into ``run_records`` (a per-run staging
area) and only the final commit transaction copies staging into ``catalog``
and advances the watermark.  This is what makes a failed or interrupted
round a no-op for readers of the published catalogue.
"""

from __future__ import annotations

import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence


def utc_now_millis() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


# A round whose staged deletions would remove at least this many existing
# catalogue entries *and* more than half of the catalogue is quarantined for
# human confirmation instead of being published.
QUARANTINE_MIN_DELETIONS = 3


class Store:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self.db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()
        self._recover_interrupted_runs()
        self._ensure_invariants()

    # ------------------------------------------------------------------ schema
    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );

                CREATE TABLE IF NOT EXISTS published_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    watermark TEXT,
                    updated_at TEXT,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    last_run_id TEXT
                );

                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL CHECK (
                        status IN ('running', 'succeeded', 'failed', 'cancelled',
                                   'awaiting_confirmation', 'rejected')
                    ),
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    window_from TEXT,
                    window_until TEXT,
                    pages_fetched INTEGER NOT NULL DEFAULT 0,
                    records_seen INTEGER NOT NULL DEFAULT 0,
                    token_restarts INTEGER NOT NULL DEFAULT 0,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    complete_list_size INTEGER,
                    phase TEXT,
                    last_error TEXT,
                    cancel_reason TEXT,
                    watermark_after TEXT,
                    quarantine_deletions INTEGER NOT NULL DEFAULT 0,
                    quarantine_catalog_before INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS run_records (
                    run_id TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    title TEXT,
                    creator TEXT,
                    datestamp TEXT NOT NULL,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    seq INTEGER NOT NULL,
                    PRIMARY KEY (run_id, identifier),
                    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS catalog (
                    identifier TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    creator TEXT,
                    source_datestamp TEXT NOT NULL,
                    harvested_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_catalog_title ON catalog(title);

                INSERT OR IGNORE INTO published_state
                    (id, watermark, updated_at, success_count, last_run_id)
                VALUES (1, NULL, NULL, 0, NULL);
                """
            )
            self._migrate_runs_table()

    def _migrate_runs_table(self) -> None:
        """Add columns / widen the status CHECK for databases from older builds.

        ``CREATE TABLE IF NOT EXISTS`` cannot change an existing table, so a
        database created before withdrawal quarantine existed still has the old
        four-status CHECK.  Rebuild the table in place, preserving rows.  The
        foreign key in ``run_records`` must keep pointing at ``runs``; with
        ``legacy_alter_table`` the rename does not rewrite that reference.
        """
        columns = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(runs)").fetchall()
        }
        if "quarantine_deletions" in columns:
            return
        self._conn.execute("PRAGMA foreign_keys=OFF")
        self._conn.execute("PRAGMA legacy_alter_table=ON")
        old_columns = [
            "id",
            "status",
            "started_at",
            "finished_at",
            "window_from",
            "window_until",
            "pages_fetched",
            "records_seen",
            "token_restarts",
            "attempt",
            "complete_list_size",
            "phase",
            "last_error",
            "cancel_reason",
            "watermark_after",
        ]
        present = [name for name in old_columns if name in columns]
        projected = ", ".join(present)
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute("ALTER TABLE runs RENAME TO runs_pre_quarantine")
            self._conn.execute(
                """
                CREATE TABLE runs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL CHECK (
                        status IN ('running', 'succeeded', 'failed', 'cancelled',
                                   'awaiting_confirmation', 'rejected')
                    ),
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    window_from TEXT,
                    window_until TEXT,
                    pages_fetched INTEGER NOT NULL DEFAULT 0,
                    records_seen INTEGER NOT NULL DEFAULT 0,
                    token_restarts INTEGER NOT NULL DEFAULT 0,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    complete_list_size INTEGER,
                    phase TEXT,
                    last_error TEXT,
                    cancel_reason TEXT,
                    watermark_after TEXT,
                    quarantine_deletions INTEGER NOT NULL DEFAULT 0,
                    quarantine_catalog_before INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            self._conn.execute(
                f"""
                INSERT INTO runs ({projected})
                SELECT {projected} FROM runs_pre_quarantine
                """
            )
            self._conn.execute("DROP TABLE runs_pre_quarantine")
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        finally:
            self._conn.execute("PRAGMA legacy_alter_table=OFF")
            self._conn.execute("PRAGMA foreign_keys=ON")

    def _recover_interrupted_runs(self) -> None:
        """A previous process died.  No in-memory task can own these runs.

        Rounds waiting for withdrawal confirmation are deliberately left
        intact: they were never published and their staging area is exactly
        what the operator must be able to confirm or reject after a restart.
        """
        with self._lock:
            now = utc_now_millis()
            rows = self._conn.execute(
                "SELECT id FROM runs WHERE status = 'running'"
            ).fetchall()
            for row in rows:
                self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'failed',
                           finished_at = ?,
                           phase = 'failed',
                           last_error = COALESCE(
                               last_error,
                               '采集进程中断，本轮未提交（已发布目录与水位保持不变）'
                           )
                     WHERE id = ? AND status = 'running'
                    """,
                    (now, row["id"]),
                )
                self._conn.execute(
                    "DELETE FROM run_records WHERE run_id = ?", (row["id"],)
                )
            self._conn.execute(
                "DELETE FROM meta WHERE key = 'active_run_id'"
            )
            # Keep blocking new rounds until the pending round is resolved,
            # but do not require the old meta key to find it.
            pending = self._conn.execute(
                "SELECT id FROM runs WHERE status = 'awaiting_confirmation'"
            ).fetchone()
            if pending is not None:
                self._meta_set("active_run_id", pending["id"])

    def _ensure_invariants(self) -> None:
        """At most one round may occupy the active slot, across processes.

        The slot is shared by ``running`` and ``awaiting_confirmation`` so a
        round pending withdrawal review blocks new rounds while a process
        restarts.  Created after recovery so a legacy database with stray
        ``running`` rows cannot make startup fail.  An index left by an older
        build only covered ``running``; it is replaced once.
        """
        with self._lock:
            existing = self._conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' "
                "AND name = 'idx_runs_one_active'"
            ).fetchone()
            if existing is not None and "awaiting_confirmation" in (
                existing["sql"] or ""
            ):
                return
            self._conn.execute("DROP INDEX IF EXISTS idx_runs_one_active")
            self._conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_active
                    ON runs((1))
                 WHERE status IN ('running', 'awaiting_confirmation')
                """
            )

    # ----------------------------------------------------------------- helpers
    def _meta_get(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def _meta_set(self, key: str, value: str | None) -> None:
        if value is None:
            self._conn.execute("DELETE FROM meta WHERE key = ?", (key,))
        else:
            self._conn.execute(
                """
                INSERT INTO meta(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    # -------------------------------------------------------------------- runs
    def create_run(self) -> dict[str, Any]:
        with self._lock:
            run_id = uuid.uuid4().hex
            now = utc_now_millis()
            state = self._conn.execute(
                "SELECT watermark FROM published_state WHERE id = 1"
            ).fetchone()
            watermark = state["watermark"] if state else None
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    INSERT INTO runs
                        (id, status, started_at, window_from, phase)
                    VALUES (?, 'running', ?, ?, 'connecting')
                    """,
                    (run_id, now, watermark),
                )
                self._meta_set("active_run_id", run_id)
                self._conn.execute("COMMIT")
            except sqlite3.IntegrityError as exc:
                self._conn.execute("ROLLBACK")
                raise RuntimeError(
                    "已有采集轮次在运行或等待撤回确认"
                ) from exc
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            run = self.get_run(run_id)
            assert run is not None
            return run

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if row is None:
                return None
            run = dict(row)
            staged = self._conn.execute(
                "SELECT COUNT(*) AS n FROM run_records WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            run["staged_count"] = int(staged["n"])
            return run

    def get_active_run(self) -> dict[str, Any] | None:
        with self._lock:
            run_id = self._meta_get("active_run_id")
            if run_id:
                run = self.get_run(run_id)
                if run and run["status"] == "running":
                    return run
            row = self._conn.execute(
                """
                SELECT * FROM runs WHERE status = 'running'
                 ORDER BY started_at DESC LIMIT 1
                """
            ).fetchone()
            return self.get_run(row["id"]) if row else None

    def get_pending_confirmation(self) -> dict[str, Any] | None:
        """The round (if any) whose withdrawals await human confirmation."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM runs WHERE status = 'awaiting_confirmation'
                 ORDER BY started_at DESC LIMIT 1
                """
            ).fetchone()
            return self.get_run(row["id"]) if row else None

    def list_runs(self, limit: int = 15) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def is_run_running(self, run_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM runs WHERE id = ? AND status = 'running'",
                (run_id,),
            ).fetchone()
            return row is not None

    def set_window(self, run_id: str, until: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE runs SET window_until = ?, phase = 'fetching'
                 WHERE id = ? AND status = 'running'
                """,
                (until, run_id),
            )
            return cur.rowcount > 0

    def update_progress(
        self,
        run_id: str,
        *,
        pages_fetched: int,
        records_seen: int,
        token_restarts: int,
        attempt: int,
        complete_list_size: int | None,
        phase: str | None = None,
    ) -> bool:
        with self._lock:
            if phase is None:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET pages_fetched = ?,
                           records_seen = ?,
                           token_restarts = ?,
                           attempt = ?,
                           complete_list_size = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (
                        pages_fetched,
                        records_seen,
                        token_restarts,
                        attempt,
                        complete_list_size,
                        run_id,
                    ),
                )
            else:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET pages_fetched = ?,
                           records_seen = ?,
                           token_restarts = ?,
                           attempt = ?,
                           complete_list_size = ?,
                           phase = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (
                        pages_fetched,
                        records_seen,
                        token_restarts,
                        attempt,
                        complete_list_size,
                        phase,
                        run_id,
                    ),
                )
            return cur.rowcount > 0

    def set_last_error(self, run_id: str, message: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE runs SET last_error = ?
                 WHERE id = ? AND status = 'running'
                """,
                (message[:4000], run_id),
            )

    def finish_cancelled(self, run_id: str, reason: str) -> bool:
        """Finalise a cancelled run; safe to call twice."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'cancelled',
                           finished_at = COALESCE(finished_at, ?),
                           phase = 'cancelled',
                           cancel_reason = COALESCE(cancel_reason, ?)
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), reason[:1000], run_id),
                )
                changed = cur.rowcount > 0
                if changed:
                    # Only the round that was actually still running loses its
                    # staging.  A round in ``awaiting_confirmation`` keeps its
                    # final staging so it can still be confirmed after a
                    # shutdown/restart.
                    self._delete_stage(run_id)
                    self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return changed
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def finish_failed(self, run_id: str, error: str) -> bool:
        """Finalise a failed run.  Staging is discarded, published data not touched."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'failed',
                           finished_at = COALESCE(finished_at, ?),
                           phase = 'failed',
                           last_error = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), error[:4000], run_id),
                )
                changed = cur.rowcount > 0
                if changed:
                    self._delete_stage(run_id)
                    self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return changed
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def reset_stage(self, run_id: str) -> bool:
        """Discard all pages collected for the current attempt."""
        with self._lock:
            if not self.is_run_running(run_id):
                return False
            self._conn.execute(
                "DELETE FROM run_records WHERE run_id = ?", (run_id,)
            )
            return True

    def stage_records(
        self, run_id: str, records: Sequence[dict[str, Any]], seq_start: int
    ) -> int:
        """Insert one page into staging, keeping one row per identifier.

        If the same identifier occurs again (whether because of a malformed
        source or a repeated page), the record with the newer OAI datestamp
        wins; the later observation wins a tie.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if not self.is_run_running(run_id):
                    self._conn.execute("ROLLBACK")
                    return 0
                for offset, record in enumerate(records):
                    seq = seq_start + offset
                    self._conn.execute(
                        """
                        INSERT INTO run_records
                            (run_id, identifier, title, creator, datestamp,
                             deleted, seq)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(run_id, identifier) DO UPDATE SET
                            title = excluded.title,
                            creator = excluded.creator,
                            datestamp = excluded.datestamp,
                            deleted = excluded.deleted,
                            seq = excluded.seq
                        WHERE excluded.datestamp > run_records.datestamp
                           OR (excluded.datestamp = run_records.datestamp
                               AND excluded.seq > run_records.seq)
                        """,
                        (
                            run_id,
                            record["identifier"],
                            record.get("title"),
                            record.get("creator"),
                            record["datestamp"],
                            1 if record.get("deleted") else 0,
                            seq,
                        ),
                    )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            return len(records)

    def _delete_stage(self, run_id: str) -> None:
        self._conn.execute("DELETE FROM run_records WHERE run_id = ?", (run_id,))

    def _clear_active_if(self, run_id: str) -> None:
        if self._meta_get("active_run_id") == run_id:
            self._meta_set("active_run_id", None)

    def _staged_rows(self, run_id: str) -> list[sqlite3.Row]:
        return self._conn.execute(
            """
            SELECT identifier, title, creator, datestamp, deleted
              FROM run_records WHERE run_id = ?
             ORDER BY seq
            """,
            (run_id,),
        ).fetchall()

    def _publish_locked(
        self, run_id: str, run: sqlite3.Row, now: str
    ) -> dict[str, Any]:
        """Copy staging into the catalogue and advance the watermark.

        Must be called inside an already-open IMMEDIATE transaction.  Returns
        the commit summary; never commits or rolls back itself, so callers can
        make publication and their own state change one atomic unit.
        """
        rows = self._staged_rows(run_id)
        upserts = 0
        deletions = 0
        for row in rows:
            if row["deleted"]:
                self._conn.execute(
                    "DELETE FROM catalog WHERE identifier = ?",
                    (row["identifier"],),
                )
                deletions += 1
            else:
                self._conn.execute(
                    """
                    INSERT INTO catalog
                        (identifier, title, creator, source_datestamp,
                         harvested_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(identifier) DO UPDATE SET
                        title = excluded.title,
                        creator = excluded.creator,
                        source_datestamp = excluded.source_datestamp,
                        harvested_at = excluded.harvested_at
                    """,
                    (
                        row["identifier"],
                        row["title"] or "",
                        row["creator"],
                        row["datestamp"],
                        now,
                    ),
                )
                upserts += 1

        watermark = run["window_until"]
        self._conn.execute(
            """
            UPDATE published_state
               SET watermark = ?,
                   updated_at = ?,
                   success_count = success_count + 1,
                   last_run_id = ?
             WHERE id = 1
            """,
            (watermark, now, run_id),
        )
        self._conn.execute(
            """
            UPDATE runs
               SET status = 'succeeded',
                   finished_at = ?,
                   phase = 'published',
                   watermark_after = ?,
                   last_error = NULL
             WHERE id = ?
            """,
            (now, watermark, run_id),
        )
        self._clear_active_if(run_id)
        self._delete_stage(run_id)
        return {
            "committed": True,
            "upserts": upserts,
            "deletions": deletions,
            "watermark": watermark,
        }

    def _withdrawal_impact_locked(self, run_id: str) -> tuple[int, int]:
        """(deletions of currently-catalogued identifiers, catalogue size)."""
        row = self._conn.execute(
            """
            SELECT COUNT(*) AS n
              FROM run_records r
             WHERE r.run_id = ?
               AND r.deleted = 1
               AND EXISTS (SELECT 1 FROM catalog c
                            WHERE c.identifier = r.identifier)
            """,
            (run_id,),
        ).fetchone()
        deletions = int(row["n"] or 0)
        size = int(
            self._conn.execute("SELECT COUNT(*) AS n FROM catalog").fetchone()["n"]
        )
        return deletions, size

    @staticmethod
    def _requires_confirmation(deletions: int, catalog_before: int) -> bool:
        return (
            deletions >= QUARANTINE_MIN_DELETIONS
            and deletions * 2 > catalog_before
        )

    def commit_run(self, run_id: str) -> dict[str, Any]:
        """Atomically publish staging and advance the watermark.

        If the staged result would delete at least ``QUARANTINE_MIN_DELETIONS``
        existing catalogue entries and more than half of the catalogue, the
        round is *not* published.  Instead it moves to
        ``awaiting_confirmation`` in this same transaction: the final staging
        rows and the time window are kept, the catalogue and watermark are
        untouched, and the round keeps occupying the active slot so no new
        round can start.  Confirmation/rejection then happens through
        :meth:`confirm_run` / :meth:`reject_run`.

        The check for ownership (``active_run_id``) happens inside the same
        IMMEDIATE transaction as the catalogue writes.  A cancelled or
        superseded round can therefore never publish, even if its HTTP
        response arrives late.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                run = self._conn.execute(
                    "SELECT * FROM runs WHERE id = ?", (run_id,)
                ).fetchone()
                active = self._meta_get("active_run_id")
                if (
                    run is None
                    or run["status"] != "running"
                    or active != run_id
                    or not run["window_until"]
                ):
                    self._conn.execute("ROLLBACK")
                    return {"committed": False, "reason": "run is not active"}

                deletions, catalog_before = self._withdrawal_impact_locked(run_id)
                if self._requires_confirmation(deletions, catalog_before):
                    self._conn.execute(
                        """
                        UPDATE runs
                           SET status = 'awaiting_confirmation',
                               finished_at = ?,
                               phase = 'awaiting_confirmation',
                               quarantine_deletions = ?,
                               quarantine_catalog_before = ?,
                               last_error = NULL
                         WHERE id = ?
                        """,
                        (
                            utc_now_millis(),
                            deletions,
                            catalog_before,
                            run_id,
                        ),
                    )
                    # Staging is intentionally retained for the confirmation
                    # step; active_run_id also remains set so restart recovery
                    # finds this round and new rounds stay blocked.
                    self._conn.execute("COMMIT")
                    return {
                        "committed": False,
                        "quarantined": True,
                        "deletions": deletions,
                        "catalog_before": catalog_before,
                    }

                result = self._publish_locked(run_id, run, utc_now_millis())
                result["quarantined"] = False
                self._conn.execute("COMMIT")
                return result
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def confirm_run(self, run_id: str) -> dict[str, Any]:
        """Publish a quarantined round atomically after human confirmation.

        Idempotent and retryable:

        * a write failure rolls back the whole transaction, so the round stays
          ``awaiting_confirmation`` with its staging intact and can be retried;
        * a repeated confirmation of an already-published round reports
          ``committed: False`` and changes nothing (no double success count);
        * a round in any other state is refused.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                run = self._conn.execute(
                    "SELECT * FROM runs WHERE id = ?", (run_id,)
                ).fetchone()
                if run is None:
                    self._conn.execute("ROLLBACK")
                    return {"committed": False, "reason": "轮次不存在"}
                if run["status"] == "succeeded":
                    self._conn.execute("ROLLBACK")
                    return {
                        "committed": False,
                        "already_committed": True,
                        "reason": "本轮已发布，无需重复确认",
                    }
                if run["status"] != "awaiting_confirmation":
                    self._conn.execute("ROLLBACK")
                    return {
                        "committed": False,
                        "reason": f"本轮状态为 {run['status']}，不在待确认状态",
                    }
                if not run["window_until"]:
                    self._conn.execute("ROLLBACK")
                    return {"committed": False, "reason": "本轮缺少时间窗"}

                result = self._publish_locked(run_id, run, utc_now_millis())
                self._conn.execute("COMMIT")
                result["quarantined"] = False
                return result
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def reject_run(self, run_id: str, reason: str = "用户驳回了批量撤回") -> bool:
        """Discard a quarantined round; catalogue and watermark stay as-is."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'rejected',
                           finished_at = COALESCE(finished_at, ?),
                           phase = 'rejected',
                           cancel_reason = COALESCE(cancel_reason, ?)
                     WHERE id = ? AND status = 'awaiting_confirmation'
                    """,
                    (utc_now_millis(), reason[:1000], run_id),
                )
                changed = cur.rowcount > 0
                if changed:
                    self._delete_stage(run_id)
                    self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return changed
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def fail_run(self, run_id: str, error: str) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'failed',
                           finished_at = ?,
                           phase = 'failed',
                           last_error = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), error[:4000], run_id),
                )
                changed = cur.rowcount > 0
                if changed:
                    self._delete_stage(run_id)
                    self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return changed
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def cancel_run(self, run_id: str, reason: str) -> bool:
        """Cancel a running round.  A pending-confirmation round is not running,
        so its retained staging is never touched here (shutdown calls this)."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'cancelled',
                           finished_at = ?,
                           phase = 'cancelled',
                           cancel_reason = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), reason[:1000], run_id),
                )
                changed = cur.rowcount > 0
                if changed:
                    self._delete_stage(run_id)
                    self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return changed
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    # --------------------------------------------------------------- published
    def pending_withdrawals(self, run_id: str | None = None) -> list[dict[str, Any]]:
        """Identifiers a quarantined round would delete from the catalogue."""
        with self._lock:
            if run_id is None:
                pending = self.get_pending_confirmation()
                if pending is None:
                    return []
                run_id = pending["id"]
            rows = self._conn.execute(
                """
                SELECT r.identifier, r.datestamp,
                       c.title AS current_title
                  FROM run_records r
                  LEFT JOIN catalog c ON c.identifier = r.identifier
                 WHERE r.run_id = ? AND r.deleted = 1
                 ORDER BY r.identifier
                """,
                (run_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def published_state(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM published_state WHERE id = 1"
            ).fetchone()
            result = dict(row) if row else {
                "watermark": None,
                "updated_at": None,
                "success_count": 0,
                "last_run_id": None,
            }
            count = self._conn.execute(
                "SELECT COUNT(*) AS n FROM catalog"
            ).fetchone()
            result["catalog_size"] = int(count["n"])
            return result

    def catalog_page(
        self, query: str = "", page: int = 1, page_size: int = 25
    ) -> dict[str, Any]:
        query = (query or "").strip()
        page = max(1, int(page or 1))
        page_size = min(200, max(1, int(page_size or 25)))
        like = f"%{query}%"
        where = ""
        params: list[Any] = []
        if query:
            where = (
                "WHERE identifier LIKE ? OR title LIKE ? "
                "OR COALESCE(creator, '') LIKE ?"
            )
            params = [like, like, like]
        with self._lock:
            total = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM catalog {where}", params
            ).fetchone()["n"]
            total = int(total)
            pages = max(1, (total + page_size - 1) // page_size)
            if page > pages:
                page = pages
            rows = self._conn.execute(
                f"""
                SELECT identifier, title, creator, source_datestamp,
                       harvested_at
                  FROM catalog {where}
                 ORDER BY title COLLATE NOCASE, identifier
                 LIMIT ? OFFSET ?
                """,
                [*params, page_size, (page - 1) * page_size],
            ).fetchall()
            return {
                "records": [dict(r) for r in rows],
                "total": total,
                "page": page,
                "page_size": page_size,
                "pages": pages,
                "query": query,
            }

    def close(self) -> None:
        with self._lock:
            self._conn.close()
