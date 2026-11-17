"""HTTP-level tests for the harvester web app (pages and APIs)."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.oai import OaiClient


@pytest.fixture
def web_app(tmp_path: Path, demo_app, repo):
    def factory() -> OaiClient:
        return OaiClient(
            "http://demo.local/oai",
            transport=httpx.ASGITransport(app=demo_app),
        )

    app = create_app(
        db_path=tmp_path / "web.sqlite3",
        oai_url="http://demo.local/oai",
        client_factory=factory,
    )
    return app


def poll_until(client: TestClient, predicate, attempts: int = 100) -> dict:
    for _ in range(attempts):
        payload = client.get("/api/status").json()
        if predicate(payload):
            return payload
        import time

        time.sleep(0.05)
    raise AssertionError(f"condition not met; last={payload}")


def test_pages_render_and_are_empty_initially(web_app):
    with TestClient(web_app) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert "已发布目录" in response.text
        assert "尚未成功采集" in response.text
        assert client.get("/runs").status_code == 200
        assert client.get("/status").status_code == 200
        status = client.get("/api/status").json()
        assert status["active"] is None
        assert status["published"]["watermark"] is None


def test_start_harvest_via_api_and_see_catalogue(web_app):
    with TestClient(web_app) as client:
        response = client.post("/api/harvest")
        assert response.status_code == 202
        payload = poll_until(client, lambda p: p["active"] is None and p["published"]["watermark"])
        assert payload["published"]["catalog_size"] > 0

        page = client.get("/")
        assert "数字人文中的文本挖掘方法" in page.text
        assert payload["published"]["watermark"] in page.text

        search = client.get("/", params={"q": "数据治理"})
        assert "高校图书馆数据治理实践" in search.text
        assert "数字人文中的文本挖掘方法" not in search.text

        catalog = client.get("/api/catalog", params={"q": "OAI-PMH"}).json()
        assert catalog["total"] == 1


def test_second_start_while_running_conflicts(web_app, repo):
    repo.set_meta("slow_seconds", "1.0")
    with TestClient(web_app) as client:
        assert client.post("/api/harvest").status_code == 202
        second = client.post("/api/harvest")
        assert second.status_code == 409
        client.post("/api/harvest/cancel")
        poll_until(client, lambda p: p["active"] is None)


def test_cancel_api_is_immediate(web_app, repo):
    repo.set_meta("slow_seconds", "5")
    with TestClient(web_app) as client:
        assert client.post("/api/harvest").status_code == 202
        poll_until(client, lambda p: p["active"] is not None)
        import time

        started = time.monotonic()
        response = client.post("/api/harvest/cancel")
        assert response.status_code == 200
        assert time.monotonic() - started < 2.0
        status = poll_until(client, lambda p: p["active"] is None)
        assert status["published"]["watermark"] is None


def test_cancel_when_idle_is_conflict(web_app):
    with TestClient(web_app) as client:
        assert client.post("/api/harvest/cancel").status_code == 409


def test_restart_while_slow_starts_new_round_immediately(web_app, repo):
    repo.set_meta("slow_seconds", "5")
    with TestClient(web_app) as client:
        first = client.post("/api/harvest")
        assert first.status_code == 202
        first_id = first.json()["run_id"]
        poll_until(client, lambda p: p["active"] is not None)

        import time

        started = time.monotonic()
        second = client.post("/api/harvest/restart")
        assert second.status_code == 202
        assert time.monotonic() - started < 2.0
        assert second.json()["run_id"] != first_id

        # The slow old round must not block the new one: turn slowness off.
        repo.set_meta("slow_seconds", "0")
        status = poll_until(
            client,
            lambda p: p["active"] is None and p["published"]["watermark"],
        )
        assert status["published"]["catalog_size"] > 0
        runs = client.get("/runs").text
        assert "cancelled" in runs


def test_runs_page_lists_attempts_and_token_restarts(web_app, repo):
    with TestClient(web_app) as client:
        client.post("/api/harvest")
        poll_until(client, lambda p: p["active"] is None and p["published"]["watermark"])
        page = client.get("/runs")
        assert page.status_code == 200
        assert "succeeded" in page.text
        assert "分页重取" in page.text


def _seed_and_quarantine(client, repo):
    client.post("/api/harvest")
    poll_until(client, lambda p: p["active"] is None and p["published"]["watermark"])
    time_sleep()
    repo.withdraw_all()
    client.post("/api/harvest")
    return poll_until(client, lambda p: p["pending_confirmation"] is not None)


def time_sleep():
    import time

    time.sleep(1.05)


def test_mass_withdrawal_is_shown_and_blocks_actions(web_app, repo):
    with TestClient(web_app) as client:
        payload = _seed_and_quarantine(client, repo)
        pending = payload["pending_confirmation"]
        assert pending["status"] == "awaiting_confirmation"
        assert pending["quarantine_deletions"] == 10
        assert pending["quarantine_catalog_before"] == 10
        assert len(payload["pending_withdrawals"]) == 10
        assert payload["active"] is None
        # Watermark untouched by the held round.
        assert payload["published"]["catalog_size"] == 10

        page = client.get("/")
        assert "撤回隔离" in page.text
        assert "oai:demo:0001" in page.text
        assert "确认发布本轮" in page.text

        # New rounds are refused while confirmation is pending.
        assert client.post("/api/harvest").status_code == 409
        assert client.post("/api/harvest/restart").status_code == 409
        assert client.post("/api/harvest/cancel").status_code == 409


def test_confirm_via_api_publishes_and_unblocks(web_app, repo):
    with TestClient(web_app) as client:
        payload = _seed_and_quarantine(client, repo)
        pending_id = payload["pending_confirmation"]["id"]
        watermark_before = payload["published"]["watermark"]

        response = client.post("/api/harvest/confirm")
        assert response.status_code == 200
        body = response.json()
        assert body["committed"] is True
        assert body["run_id"] == pending_id

        status = client.get("/api/status").json()
        assert status["pending_confirmation"] is None
        assert status["published"]["catalog_size"] == 0
        assert status["published"]["watermark"] != watermark_before
        assert status["published"]["watermark"] == payload["pending_confirmation"]["window_until"]

        # Confirming again is rejected as there is no pending round; a new
        # round can start (and immediately has an empty window).
        assert client.post("/api/harvest/confirm").status_code == 409
        assert client.post("/api/harvest").status_code == 202
        poll_until(client, lambda p: p["active"] is None)


def test_reject_via_api_restores_ability_to_harvest(web_app, repo):
    with TestClient(web_app) as client:
        payload = _seed_and_quarantine(client, repo)
        before_ids = payload["published"]
        response = client.post("/api/harvest/reject")
        assert response.status_code == 200
        assert response.json()["status"] == "rejected"

        status = client.get("/api/status").json()
        assert status["pending_confirmation"] is None
        assert status["published"]["watermark"] == before_ids["watermark"]
        assert status["published"]["catalog_size"] == 10
        assert client.get("/").status_code == 200
        assert client.post("/api/harvest").status_code == 202
