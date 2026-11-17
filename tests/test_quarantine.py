"""Withdrawal quarantine: a round that would mass-delete is held, not published.

Rules under test:

* a completed round that would delete at least 3 *existing* catalogue entries
  and more than half of the catalogue is held in ``awaiting_confirmation``;
  catalogue, watermark and success count do not change and no new round may
  start;
* the pending round survives a process restart and can still be confirmed or
  rejected;
* confirmation publishes the staging and watermark atomically, is idempotent,
  and a write failure leaves a retryable pending state (never a half publish);
* rejection discards staging and allows re-harvesting from the original
  watermark;
* a round that stays below the thresholds publishes normally;
* token-failure restarts do not double-count withdrawals from discarded
  attempts.
"""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

from app.harvest import HarvestConfig, Harvester
from app.oai import OaiPage, OaiRecord
from app.store import Store
from conftest import wait_for_outcome


def catalog_ids(store: Store) -> set[str]:
    return {item["identifier"] for item in store.catalog_page(page_size=200)["records"]}


async def seed_baseline(harvester, store):
    first = await harvester.start_run()
    assert await wait_for_outcome(first) == "succeeded"
    return store.published_state()


def stage_deletions(store: Store, run_id: str, identifiers, datestamp="2026-01-01T00:00:00Z"):
    store.set_window(run_id, "2026-01-02T00:00:00Z")
    store.stage_records(
        run_id,
        [
            {
                "identifier": identifier,
                "title": None,
                "creator": None,
                "datestamp": datestamp,
                "deleted": True,
            }
            for identifier in identifiers
        ],
        0,
    )


# ------------------------------------------------------------------ thresholds
async def test_mass_withdrawal_is_held_and_blocks_new_rounds(harvester, store, repo):
    baseline = await seed_baseline(harvester, store)
    before_ids = catalog_ids(store)

    await asyncio.sleep(1.05)
    repo.withdraw_all()  # 10 deletions, all currently catalogued

    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "awaiting_confirmation"

    run = store.get_run(control.run_id)
    assert run["status"] == "awaiting_confirmation"
    assert run["quarantine_deletions"] == 10
    assert run["quarantine_catalog_before"] == 10
    assert run["staged_count"] == 10  # final staging retained
    assert run["window_from"] == baseline["watermark"]
    assert run["window_until"]

    state = store.published_state()
    assert state["watermark"] == baseline["watermark"]
    assert state["catalog_size"] == baseline["catalog_size"]
    assert state["success_count"] == baseline["success_count"]
    assert catalog_ids(store) == before_ids

    # The active slot is occupied: neither a plain start nor a restart works.
    assert store.get_active_run() is None  # no round is *running* any more
    assert store.get_pending_confirmation()["id"] == control.run_id
    with pytest.raises(RuntimeError, match="待撤回确认"):
        await harvester.start_run()

    pending = store.pending_withdrawals(control.run_id)
    assert {item["identifier"] for item in pending} == before_ids
    assert all(item["current_title"] for item in pending)


async def test_two_deletions_publish_normally(harvester, store, repo):
    await seed_baseline(harvester, store)
    await asyncio.sleep(1.05)
    repo.withdraw("oai:demo:0001")
    repo.withdraw("oai:demo:0002")

    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "succeeded"
    assert store.get_run(control.run_id)["status"] == "succeeded"
    assert store.published_state()["catalog_size"] == 8
    assert "oai:demo:0001" not in catalog_ids(store)
    assert store.get_pending_confirmation() is None


async def test_many_deletions_without_majority_publish_normally(harvester, store, repo):
    """3+ deletions but not more than half the catalogue: no quarantine."""
    for i in range(4, 10):
        repo.add_record(title=f"额外记录 {i}", creator=None)
    first = await harvester.start_run()
    assert await wait_for_outcome(first) == "succeeded"
    assert store.published_state()["catalog_size"] == 16

    await asyncio.sleep(1.05)
    for identifier in ("oai:demo:0001", "oai:demo:0002", "oai:demo:0003", "oai:demo:0004"):
        repo.withdraw(identifier)

    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "succeeded"
    assert store.published_state()["catalog_size"] == 12


async def test_three_of_five_is_quarantined_and_three_of_six_is_not(store):
    """Boundary check of the strict-majority rule at store level."""
    now = "2026-01-02T00:00:00Z"
    for catalog_size, deletions, expected in ((5, 3, True), (6, 3, False)):
        db_store = Store(store.db_path.parent / f"boundary-{catalog_size}.sqlite3")
        run = db_store.create_run()
        db_store.set_window(run["id"], "2025-12-31T00:00:00Z")
        db_store.stage_records(
            run["id"],
            [
                {
                    "identifier": f"oai:b:{i}",
                    "title": f"记录 {i}",
                    "creator": None,
                    "datestamp": "2025-12-30T00:00:00Z",
                    "deleted": False,
                }
                for i in range(catalog_size)
            ],
            0,
        )
        assert db_store.commit_run(run["id"])["committed"]

        second = db_store.create_run()
        stage_deletions(db_store, second["id"], [f"oai:b:{i}" for i in range(deletions)], now)
        result = db_store.commit_run(second["id"])
        assert bool(result.get("quarantined")) is expected
        if expected:
            assert db_store.get_run(second["id"])["status"] == "awaiting_confirmation"
            assert db_store.published_state()["catalog_size"] == catalog_size
        else:
            assert db_store.published_state()["catalog_size"] == catalog_size - deletions
        db_store.close()


# ----------------------------------------------------------------- confirmation
async def test_confirm_publishes_catalogue_and_watermark_once(harvester, store, repo):
    baseline = await seed_baseline(harvester, store)
    await asyncio.sleep(1.05)
    repo.withdraw_all()
    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "awaiting_confirmation"
    pending = store.get_pending_confirmation()

    result = store.confirm_run(control.run_id)
    assert result["committed"] is True
    assert result["deletions"] == 10

    state = store.published_state()
    assert state["catalog_size"] == 0
    assert state["success_count"] == baseline["success_count"] + 1
    assert state["watermark"] == pending["window_until"]
    assert state["watermark"] != baseline["watermark"]
    run = store.get_run(control.run_id)
    assert run["status"] == "succeeded"
    assert run["staged_count"] == 0
    assert store.get_pending_confirmation() is None

    # Repeating the confirmation must not publish or count twice.
    again = store.confirm_run(control.run_id)
    assert again["committed"] is False
    assert again.get("already_committed") is True
    after = store.published_state()
    assert after["success_count"] == state["success_count"]
    assert after["watermark"] == state["watermark"]
    assert after["catalog_size"] == 0


async def test_reject_keeps_catalogue_and_allows_reharvest(harvester, store, repo):
    baseline = await seed_baseline(harvester, store)
    before_ids = catalog_ids(store)
    await asyncio.sleep(1.05)
    repo.withdraw_all()
    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "awaiting_confirmation"

    assert store.reject_run(control.run_id) is True
    run = store.get_run(control.run_id)
    assert run["status"] == "rejected"
    assert run["staged_count"] == 0
    state = store.published_state()
    assert state["watermark"] == baseline["watermark"]
    assert catalog_ids(store) == before_ids
    assert store.get_pending_confirmation() is None
    assert store.get_active_run() is None

    # Re-harvesting from the original watermark is possible; the withdrawals
    # are still on the source, so this round is quarantined again.
    retry = await harvester.start_run()
    assert await wait_for_outcome(retry) == "awaiting_confirmation"
    assert store.get_run(retry.run_id)["window_from"] == baseline["watermark"]


async def test_confirm_write_failure_keeps_retryable_pending_state(harvester, store, repo, monkeypatch):
    baseline = await seed_baseline(harvester, store)
    before_ids = catalog_ids(store)
    await asyncio.sleep(1.05)
    repo.withdraw_all()
    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "awaiting_confirmation"
    pending = store.get_pending_confirmation()

    class ExplodingConnection:
        def __init__(self, inner):
            self._inner = inner
            self.deletes = 0

        def execute(self, sql, *args, **kwargs):
            if sql.strip().startswith("DELETE FROM catalog"):
                self.deletes += 1
                if self.deletes == 4:
                    raise sqlite3.OperationalError("simulated disk error")
            return self._inner.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    real_conn = store._conn
    monkeypatch.setattr(store, "_conn", ExplodingConnection(real_conn))
    with pytest.raises(sqlite3.OperationalError):
        store.confirm_run(control.run_id)
    monkeypatch.setattr(store, "_conn", real_conn)

    # Nothing was published, the staging is intact and the round is still pending.
    state = store.published_state()
    assert state["watermark"] == baseline["watermark"]
    assert state["success_count"] == baseline["success_count"]
    assert catalog_ids(store) == before_ids
    still = store.get_run(control.run_id)
    assert still["status"] == "awaiting_confirmation"
    assert still["staged_count"] == pending["staged_count"]
    assert store.get_pending_confirmation()["id"] == control.run_id

    # A retry from the same final staging succeeds exactly once.
    retried = store.confirm_run(control.run_id)
    assert retried["committed"] is True
    assert store.published_state()["catalog_size"] == 0
    assert store.published_state()["success_count"] == baseline["success_count"] + 1


async def test_pending_round_survives_restart_and_can_be_confirmed(tmp_path, repo):
    db = tmp_path / "restart-pending.sqlite3"
    store = Store(db)
    # Seed a catalogue with a normal committed round.
    seed = store.create_run()
    store.set_window(seed["id"], "2025-12-31T00:00:00Z")
    store.stage_records(
        seed["id"],
        [
            {
                "identifier": f"oai:r:{i}",
                "title": f"记录 {i}",
                "creator": None,
                "datestamp": "2025-12-30T00:00:00Z",
                "deleted": False,
            }
            for i in range(3)
        ],
        0,
    )
    assert store.commit_run(seed["id"])["committed"]
    # A completed round withdrawing all three is held for confirmation.
    pending_run = store.create_run()
    stage_deletions(store, pending_run["id"], [f"oai:r:{i}" for i in range(3)])
    result = store.commit_run(pending_run["id"])
    assert result["quarantined"] is True
    pending_id = pending_run["id"]
    watermark_before = store.published_state()["watermark"]
    store._conn.close()  # hard process death

    reopened = Store(db)
    pending = reopened.get_pending_confirmation()
    assert pending is not None and pending["id"] == pending_id
    assert pending["staged_count"] == 3
    assert reopened.published_state()["watermark"] == watermark_before
    assert reopened.published_state()["catalog_size"] == 3

    # New rounds stay blocked after restart.
    active = Harvester(reopened, HarvestConfig(oai_url="scripted"), client_factory=lambda: None)
    with pytest.raises(RuntimeError, match="待撤回确认"):
        await active.start_run()

    assert reopened.confirm_run(pending_id)["committed"] is True
    assert reopened.published_state()["catalog_size"] == 0
    reopened.close()


# --------------------------------------------------------------- token retries
class ScriptedClient:
    def __init__(self, script):
        self.script = list(script)
        self.calls: list[dict] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def identify_time(self):
        return "2026-01-02T00:00:10Z"

    async def list_records(self, *, from_, until, token=None):
        self.calls.append({"from": from_, "until": until, "token": token})
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


def deletion(identifier, datestamp="2026-01-01T00:00:05Z"):
    return OaiRecord(identifier=identifier, datestamp=datestamp, deleted=True)


async def test_token_restart_counts_only_final_attempt(store):
    """A discarded attempt's withdrawals are not mixed into the final count.

    Attempt 1 stages 4 deletions before its token is rejected.  Attempt 2
    re-fetches the same window and stages a single deletion.  The catalogue
    holds 5 rows, so the final attempt (1 deletion) must publish normally:
    counting the discarded attempt would incorrectly quarantine the round.
    """
    from app.oai import BadResumptionToken

    client = ScriptedClient(
        [
            # Attempt 1: four deletions, then the token is rejected.
            OaiPage(
                token="t1",
                complete_list_size=5,
                records=[deletion(f"oai:q:{i}") for i in range(1, 5)],
            ),
            BadResumptionToken("expired"),
            # Attempt 2 from page 1: only a single deletion in this attempt.
            OaiPage(complete_list_size=1, records=[deletion("oai:q:5")]),
        ]
    )
    harvester = Harvester(
        store,
        HarvestConfig(oai_url="scripted", max_attempts=3, retry_backoff_ms=0),
        client_factory=lambda: client,
    )
    seed = store.create_run()
    store.set_window(seed["id"], "2025-12-31T00:00:00Z")
    store.stage_records(
        seed["id"],
        [
            {
                "identifier": f"oai:q:{i}",
                "title": f"记录 {i}",
                "creator": None,
                "datestamp": "2025-12-30T00:00:00Z",
                "deleted": False,
            }
            for i in range(1, 6)
        ],
        0,
    )
    assert store.commit_run(seed["id"])["committed"]

    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "succeeded"
    run = store.get_run(control.run_id)
    assert run["token_restarts"] == 1
    assert run["status"] == "succeeded"
    assert store.get_pending_confirmation() is None
    assert store.published_state()["catalog_size"] == 4  # only oai:q:5 deleted
    assert "oai:q:1" in catalog_ids(store)


async def test_token_restart_final_attempt_still_quarantines(store):
    """The final attempt itself may cross the threshold; then it is held."""
    from app.oai import BadResumptionToken

    client = ScriptedClient(
        [
            OaiPage(token="t1", complete_list_size=9, records=[deletion("oai:h:1")]),
            BadResumptionToken("expired"),
            OaiPage(
                complete_list_size=5,
                records=[deletion(f"oai:h:{i}") for i in range(1, 6)],
            ),
        ]
    )
    harvester = Harvester(
        store,
        HarvestConfig(oai_url="scripted", max_attempts=3, retry_backoff_ms=0),
        client_factory=lambda: client,
    )
    seed = store.create_run()
    store.set_window(seed["id"], "2025-12-31T00:00:00Z")
    store.stage_records(
        seed["id"],
        [
            {
                "identifier": f"oai:h:{i}",
                "title": f"记录 {i}",
                "creator": None,
                "datestamp": "2025-12-30T00:00:00Z",
                "deleted": False,
            }
            for i in range(1, 7)
        ],
        0,
    )
    assert store.commit_run(seed["id"])["committed"]

    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "awaiting_confirmation"
    run = store.get_run(control.run_id)
    assert run["quarantine_deletions"] == 5  # 5 of 6, not 6 of 6
    assert run["quarantine_catalog_before"] == 6
    assert store.published_state()["catalog_size"] == 6


async def test_shutdown_and_finalisers_do_not_clear_pending_staging(harvester, store, repo):
    """A service stop or a stray finaliser must not destroy what we must confirm."""
    await seed_baseline(harvester, store)
    await asyncio.sleep(1.05)
    repo.withdraw_all()
    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "awaiting_confirmation"
    pending_id = control.run_id

    # These are the paths Harvester.shutdown() / a late task uses.
    assert store.cancel_run(pending_id, "服务停止") is False
    assert store.finish_cancelled(pending_id, "服务停止") is False
    assert store.finish_failed(pending_id, "服务停止") is False
    assert store.fail_run(pending_id, "服务停止") is False

    pending = store.get_pending_confirmation()
    assert pending is not None and pending["id"] == pending_id
    assert pending["staged_count"] == 10
    assert store.confirm_run(pending_id)["committed"] is True
    assert store.published_state()["catalog_size"] == 0
