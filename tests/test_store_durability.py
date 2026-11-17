"""Durability across process restarts.

A ``running`` row left behind by a killed process must never be treated as
publishable, and must not block new rounds forever.
"""

from __future__ import annotations

import sqlite3

import pytest

from app.store import Store


def test_interrupted_run_is_failed_on_startup_without_publishing(tmp_path):
    db = tmp_path / "restart.sqlite3"
    first = Store(db)
    run = first.create_run()
    first.set_window(run["id"], "2026-01-01T00:00:00Z")
    first.stage_records(
        run["id"],
        [
            {
                "identifier": "oai:staged:1",
                "title": "未提交记录",
                "creator": None,
                "datestamp": "2025-12-31T00:00:00Z",
                "deleted": False,
            }
        ],
        0,
    )
    assert first.get_run(run["id"])["status"] == "running"
    # Simulate a hard process death: do not close cleanly, just drop the object.
    first._conn.close()

    second = Store(db)  # startup recovery
    recovered = second.get_run(run["id"])
    assert recovered["status"] == "failed"
    assert recovered["staged_count"] == 0
    assert "中断" in (recovered["last_error"] or "")
    assert second.get_active_run() is None
    assert second.published_state()["watermark"] is None
    assert second.published_state()["catalog_size"] == 0
    # A new round can start immediately.
    fresh = second.create_run()
    assert second.get_run(fresh["id"])["status"] == "running"
    second.close()


def test_commit_and_watermark_are_one_transaction(tmp_path, monkeypatch):
    """If the catalogue write fails mid-commit, nothing is published."""
    db = tmp_path / "atomic.sqlite3"
    store = Store(db)
    run = store.create_run()
    store.set_window(run["id"], "2026-01-01T00:00:00Z")
    store.stage_records(
        run["id"],
        [
            {
                "identifier": f"oai:atomic:{i}",
                "title": f"记录 {i}",
                "creator": None,
                "datestamp": "2025-12-31T00:00:00Z",
                "deleted": False,
            }
            for i in range(5)
        ],
        0,
    )

    class ExplodingConnection:
        def __init__(self, inner):
            self._inner = inner
            self._inserts = 0

        def execute(self, sql, *args, **kwargs):
            if sql.strip().startswith("INSERT INTO catalog"):
                self._inserts += 1
                if self._inserts == 3:
                    raise sqlite3.OperationalError("simulated disk error")
            return self._inner.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    real_conn = store._conn
    monkeypatch.setattr(store, "_conn", ExplodingConnection(real_conn))
    with pytest.raises(sqlite3.OperationalError):
        store.commit_run(run["id"])
    monkeypatch.setattr(store, "_conn", real_conn)

    state = store.published_state()
    assert state["watermark"] is None
    assert state["catalog_size"] == 0
    assert store.get_run(run["id"])["status"] == "running"  # still open for retry/cleanup
    store.finish_failed(run["id"], "simulated")
    store.close()


def test_legacy_schema_is_migrated_without_losing_runs(tmp_path):
    """A database from before withdrawal quarantine opens and gains new states."""
    db = tmp_path / "legacy.sqlite3"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE runs (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL CHECK (
                status IN ('running', 'succeeded', 'failed', 'cancelled')
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
            watermark_after TEXT
        );
        CREATE TABLE run_records (
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
        INSERT INTO runs (id, status, started_at, window_until, phase)
        VALUES ('legacy1', 'succeeded', '2026-01-01T00:00:00.000Z',
                '2026-01-01T00:00:10Z', 'published');
        INSERT INTO run_records (run_id, identifier, datestamp, deleted, seq)
        VALUES ('legacy1', 'oai:legacy:1', '2026-01-01T00:00:00Z', 0, 0);
        """
    )
    conn.commit()
    conn.close()

    store = Store(db)
    runs = store.list_runs()
    assert [run["id"] for run in runs] == ["legacy1"]
    assert runs[0]["status"] == "succeeded"
    assert runs[0]["quarantine_deletions"] == 0
    # The foreign key from run_records still points at runs.
    fk_target = store._conn.execute(
        "SELECT \"table\" FROM pragma_foreign_key_list('run_records')"
    ).fetchone()
    assert fk_target["table"] == "runs"
    # New quarantine statuses are now accepted.
    run = store.create_run()
    store.set_window(run["id"], "2026-01-02T00:00:00Z")
    store._conn.execute(
        "UPDATE runs SET status = 'awaiting_confirmation' WHERE id = ?", (run["id"],)
    )
    assert store.get_pending_confirmation()["id"] == run["id"]
    store.close()
