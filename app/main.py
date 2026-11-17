"""Web application for the institutional-repository harvester.

Pages
-----
``/``                          catalogue browser, watermark, run controls
``/runs``                      round history (windows, attempts, restarts, errors)
``/status``                    statically rendered status JSON
``GET  /api/status``           polling endpoint used by the page
``GET  /api/catalog``          catalogue JSON (search + pagination)
``POST /api/harvest``          start a round
``POST /api/harvest/cancel``   cancel the active round without waiting for it
``POST /api/harvest/restart``  cancel (if any) and immediately start a new round
``POST /api/harvest/confirm``  atomically publish a round held for withdrawal review
``POST /api/harvest/reject``   discard such a round; catalogue/watermark untouched
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .harvest import HarvestConfig, Harvester
from .store import QUARANTINE_MIN_DELETIONS, Store

BASE_DIR = Path(__file__).resolve().parent


def _data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "./data"))


def _oai_url() -> str:
    return os.environ.get("OAI_URL", "http://demo-oai:8000/oai")


def create_app(
    *,
    db_path: str | Path | None = None,
    oai_url: str | None = None,
    read_timeout: float | None = None,
    max_attempts: int | None = None,
    client_factory: Any = None,
) -> FastAPI:
    data_dir = _data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    store = Store(db_path or (data_dir / "harvester.sqlite3"))
    config = HarvestConfig(
        oai_url=oai_url or _oai_url(),
        read_timeout=float(
            read_timeout
            if read_timeout is not None
            else os.environ.get("OAI_READ_TIMEOUT", "20")
        ),
        connect_timeout=float(os.environ.get("OAI_CONNECT_TIMEOUT", "10")),
        max_attempts=int(
            max_attempts
            if max_attempts is not None
            else os.environ.get("HARVEST_MAX_ATTEMPTS", "3")
        ),
        retry_backoff_ms=int(os.environ.get("HARVEST_RETRY_BACKOFF_MS", "300")),
    )
    harvester = Harvester(store, config, client_factory=client_factory)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # A fresh process owns no in-flight rounds; Store already marked any
        # 'running' rows from a previous process as failed without publishing.
        yield
        await harvester.shutdown()
        store.close()

    app = FastAPI(title="机构库采集控制台", lifespan=lifespan)
    app.state.store = store
    app.state.harvester = harvester
    app.state.config = config
    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    static_dir = BASE_DIR / "static"
    static_dir.mkdir(exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    def status_payload() -> dict[str, Any]:
        active = store.get_active_run()
        pending = store.get_pending_confirmation()
        published = store.published_state()
        last = store.list_runs(limit=1)
        return {
            "active": active,
            "pending_confirmation": pending,
            "pending_withdrawals": (
                store.pending_withdrawals(pending["id"]) if pending else []
            ),
            "published": published,
            "last_run": last[0] if last else None,
            "config": {
                "oai_url": config.oai_url,
                "max_attempts": config.max_attempts,
                "read_timeout": config.read_timeout,
                "quarantine_min_deletions": QUARANTINE_MIN_DELETIONS,
            },
        }

    @app.get("/", response_class=HTMLResponse)
    async def index(
        request: Request,
        q: str = "",
        page: int = 1,
        page_size: int = 20,
        message: str | None = None,
    ) -> HTMLResponse:
        catalogue = store.catalog_page(q, page, page_size)
        pending = store.get_pending_confirmation()
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "catalogue": catalogue,
                "published": store.published_state(),
                "status": status_payload(),
                "runs": store.list_runs(limit=10),
                "pending": pending,
                "pending_withdrawals": (
                    store.pending_withdrawals(pending["id"]) if pending else []
                ),
                "message": message,
            },
        )

    @app.get("/runs", response_class=HTMLResponse)
    async def runs_page(request: Request) -> HTMLResponse:
        pending = store.get_pending_confirmation()
        return templates.TemplateResponse(
            request,
            "runs.html",
            {
                "runs": store.list_runs(limit=50),
                "published": store.published_state(),
                "status": status_payload(),
                "pending": pending,
            },
        )

    @app.get("/api/status")
    async def api_status() -> JSONResponse:
        return JSONResponse(status_payload())

    @app.get("/status")
    async def status_page(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request, "status.html", {"status": status_payload()}
        )

    @app.post("/api/harvest")
    async def start_harvest() -> JSONResponse:
        try:
            control = await harvester.start_run()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return JSONResponse(
            {"run_id": control.run_id, "status": "running"}, status_code=202
        )

    @app.post("/api/harvest/cancel")
    async def cancel_harvest() -> JSONResponse:
        active = store.get_active_run()
        if active is None:
            if store.get_pending_confirmation() is not None:
                raise HTTPException(
                    status_code=409,
                    detail="采集已结束，本轮正等待撤回确认；请确认或驳回，不能取消",
                )
            raise HTTPException(status_code=409, detail="当前没有正在运行的采集轮次")
        await harvester.cancel_run(active["id"], "用户在网页取消", "user")
        return JSONResponse({"run_id": active["id"], "status": "cancelled"})

    @app.post("/api/harvest/restart")
    async def restart_harvest() -> JSONResponse:
        """Cancel the active round (if any) and immediately start a new one.

        ``cancel_run`` returns as soon as SQLite has recorded the cancellation
        and the old asyncio task has been asked to stop; it never waits for a
        slow HTTP page.  The new run gets a new ``run_id`` and its own window,
        and the commit ownership check makes the old round unable to publish.
        """
        active = store.get_active_run()
        if active is not None:
            await harvester.cancel_run(
                active["id"], "用户取消了本轮以开始新一轮", "superseded"
            )
        try:
            control = await harvester.start_run()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return JSONResponse(
            {"run_id": control.run_id, "status": "running"}, status_code=202
        )

    @app.post("/api/harvest/confirm")
    async def confirm_withdrawals() -> JSONResponse:
        """Publish a quarantined round and advance the watermark, atomically.

        A write failure leaves the round in ``awaiting_confirmation`` with its
        staging intact, so the request can simply be retried.
        """
        pending = store.get_pending_confirmation()
        if pending is None:
            raise HTTPException(status_code=409, detail="没有待确认的撤回轮次")
        try:
            result = store.confirm_run(pending["id"])
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"确认发布失败，本轮仍待确认，可重试：{exc}",
            ) from exc
        return JSONResponse(
            {
                "run_id": pending["id"],
                "status": "succeeded" if result.get("committed") else pending["status"],
                **result,
            }
        )

    @app.post("/api/harvest/reject")
    async def reject_withdrawals() -> JSONResponse:
        """Discard the quarantined round; the catalogue and watermark stay."""
        pending = store.get_pending_confirmation()
        if pending is None:
            raise HTTPException(status_code=409, detail="没有待确认的撤回轮次")
        changed = store.reject_run(pending["id"])
        if not changed:
            raise HTTPException(status_code=409, detail="该轮次已不是待确认状态")
        return JSONResponse(
            {
                "run_id": pending["id"],
                "status": "rejected",
                "watermark": store.published_state()["watermark"],
            }
        )

    @app.get("/api/catalog")
    async def api_catalog(q: str = "", page: int = 1, page_size: int = 20) -> JSONResponse:
        return JSONResponse(store.catalog_page(q, page, page_size))

    return app


app = create_app()
