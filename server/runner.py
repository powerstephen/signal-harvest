"""Single-session runner with SSE broadcast."""

import asyncio
from datetime import datetime, timezone
from typing import Optional

from db.session import get_db
from db.models import ProspectSession, Prospect
from scraper.engine import run_domain_mode, run_search_mode


class _Hub:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.current_session_id: Optional[int] = None
        self.task: Optional[asyncio.Task] = None
        self.subscribers: list[asyncio.Queue] = []
        self.history_buffer: list[dict] = []

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=2000)
        for ev in self.history_buffer:
            try:
                q.put_nowait(ev)
            except asyncio.QueueFull:
                break
        self.subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        if q in self.subscribers:
            self.subscribers.remove(q)

    async def broadcast(self, event: dict):
        self.history_buffer.append(event)
        if len(self.history_buffer) > 2000:
            self.history_buffer = self.history_buffer[-1500:]
        dead = []
        for q in self.subscribers:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            self.unsubscribe(q)

    def is_running(self) -> bool:
        return self.task is not None and not self.task.done()


hub = _Hub()


async def _job(session_id: int, params: dict):
    async def log_cb(msg: str):
        await hub.broadcast({"type": "log", "msg": msg})

    leads_count = 0
    mode = params.get("mode", "domain")

    try:
        if mode == "domain":
            domains = params.get("domains", [])
            gen = run_domain_mode(
                domains=[(d[0], d[1]) for d in domains],
                country=params.get("country", ""),
                notes=params.get("notes", ""),
                log_cb=log_cb,
            )
        else:
            gen = run_search_mode(
                icp=params.get("icp", ""),
                country=params.get("country", ""),
                notes=params.get("notes", ""),
                limit=params.get("limit", 10),
                log_cb=log_cb,
            )

        async for prospect in gen:
            with get_db() as db:
                row = Prospect(session_id=session_id, **prospect)
                db.add(row)
                db.flush()
                payload = {"id": row.id, "session_id": session_id, **prospect}
            leads_count += 1
            await hub.broadcast({"type": "prospect", "prospect": payload})

        status = "completed"
    except asyncio.CancelledError:
        status = "cancelled"
        await hub.broadcast({"type": "log", "msg": "Cancelled."})
        raise
    except Exception as e:
        status = "error"
        await hub.broadcast({"type": "log", "msg": f"ERROR: {e}"})
    finally:
        with get_db() as db:
            s = db.get(ProspectSession, session_id)
            if s:
                s.status = status
                s.lead_count = leads_count
                s.finished_at = datetime.now(timezone.utc)
        await hub.broadcast({
            "type": "done",
            "session_id": session_id,
            "status": status,
            "lead_count": leads_count
        })
        hub.current_session_id = None
        hub.history_buffer.clear()


async def start(params: dict) -> int:
    async with hub.lock:
        if hub.is_running():
            raise RuntimeError("A session is already running.")
        with get_db() as db:
            s = ProspectSession(
                client_name=params.get("client_name", ""),
                mode=params.get("mode", "domain"),
                input_data=params.get("input_data", ""),
                country=params.get("country", ""),
                notes=params.get("notes", ""),
                status="running",
            )
            db.add(s)
            db.flush()
            session_id = s.id

        hub.current_session_id = session_id
        hub.history_buffer.clear()
        await hub.broadcast({"type": "start", "session_id": session_id})
        hub.task = asyncio.create_task(_job(session_id, params))
        return session_id


async def cancel():
    if hub.task and not hub.task.done():
        hub.task.cancel()
