import asyncio
import csv
import io
import json
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from db.session import init_db, get_db
from db.models import ProspectSession, Prospect
from server import runner
from scraper.yellowpages import search_yellowpages

UI_DIR = Path(__file__).resolve().parent.parent / "ui"

app = FastAPI(title="Signal Harvest")


@app.on_event("startup")
async def _startup():
    init_db()


class DomainParams(BaseModel):
    mode: str = "domain"
    client_name: str = ""
    domains: list[list[str]] = []
    country: str = ""
    notes: str = ""


class SearchParams(BaseModel):
    mode: str = "search"
    client_name: str = ""
    icp: str = Field(min_length=2, max_length=500)
    country: str = ""
    notes: str = ""
    limit: int = Field(default=10, ge=1, le=25)


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse((UI_DIR / "index.html").read_text(encoding="utf-8"))


@app.post("/api/run")
async def run_session(request: Request):
    body = await request.json()
    mode = body.get("mode", "domain")
    try:
        session_id = await runner.start(body)
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {"session_id": session_id}


@app.post("/api/cancel")
async def cancel():
    await runner.cancel()
    return {"ok": True}


@app.get("/api/status")
async def status():
    return {
        "running": runner.hub.is_running(),
        "session_id": runner.hub.current_session_id,
    }



@app.post("/api/yp")
async def yp_search(request: Request):
    body = await request.json()
    industry = body.get("industry", "")
    location = body.get("location", "")
    limit = int(body.get("limit", 20))
    client_name = body.get("client_name", "")

    if not industry or not location:
        raise HTTPException(400, "industry and location required")

    async def _run():
        async def log_cb(msg):
            await runner.hub.broadcast({"type": "log", "msg": msg})

        runner.hub.history_buffer.clear()
        await runner.hub.broadcast({"type": "start", "session_id": 0})

        try:
            results = await search_yellowpages(industry, location, limit, log_cb)
            # Save to DB
            with get_db() as db:
                s = ProspectSession(
                    client_name=client_name or f"{industry} - {location}",
                    mode="yellowpages",
                    input_data=f"{industry} | {location}",
                    status="completed",
                    lead_count=len(results),
                )
                db.add(s)
                db.flush()
                session_id = s.id
                for r in results:
                    row = Prospect(
                        session_id=session_id,
                        company=r.get("company",""),
                        website=r.get("website",""),
                        industry=r.get("industry",""),
                        employee_count=r.get("employee_count",""),
                        description=r.get("description",""),
                        country=r.get("country","US"),
                        first_name=r.get("first_name",""),
                        last_name=r.get("last_name",""),
                        email=r.get("email",""),
                        phone=r.get("phone",""),
                        job_title=r.get("job_title",""),
                        linkedin_url=r.get("linkedin_url",""),
                        signal=r.get("signal",""),
                        relevance_score=r.get("relevance_score",0.0),
                        relevance_reason=r.get("relevance_reason",""),
                        source_url=r.get("source_url",""),
                    )
                    db.add(row)
                    await runner.hub.broadcast({"type": "prospect", "prospect": {**r, "id": 0, "session_id": session_id}})

            await runner.hub.broadcast({"type": "done", "session_id": session_id, "status": "completed", "lead_count": len(results)})
        except Exception as e:
            await runner.hub.broadcast({"type": "log", "msg": f"Error: {e}"})
            await runner.hub.broadcast({"type": "done", "session_id": 0, "status": "error", "lead_count": 0})

    asyncio.create_task(_run())
    return {"ok": True}

@app.get("/api/stream")
async def stream(request: Request):
    q = runner.hub.subscribe()

    async def gen():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield {"event": ev["type"], "data": json.dumps(ev)}
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": "{}"}
        finally:
            runner.hub.unsubscribe(q)

    return EventSourceResponse(gen())


@app.get("/api/sessions")
async def list_sessions():
    with get_db() as db:
        rows = db.query(ProspectSession).order_by(ProspectSession.started_at.desc()).limit(100).all()
        return [
            {
                "id": r.id,
                "client_name": r.client_name,
                "mode": r.mode,
                "status": r.status,
                "lead_count": r.lead_count,
                "started_at": r.started_at.isoformat() if r.started_at else None,
            }
            for r in rows
        ]


@app.get("/api/sessions/{session_id}")
async def get_session(session_id: int):
    with get_db() as db:
        s = db.get(ProspectSession, session_id)
        if not s:
            raise HTTPException(404, "Not found")
        prospects = db.query(Prospect).filter(Prospect.session_id == session_id).order_by(Prospect.relevance_score.desc()).all()
        return {
            "session": {
                "id": s.id, "client_name": s.client_name, "mode": s.mode,
                "status": s.status, "lead_count": s.lead_count,
                "started_at": s.started_at.isoformat() if s.started_at else None,
            },
            "prospects": [
                {
                    "id": p.id, "company": p.company, "website": p.website,
                    "industry": p.industry, "employee_count": p.employee_count,
                    "description": p.description, "country": p.country,
                    "first_name": p.first_name, "last_name": p.last_name,
                    "email": p.email, "phone": p.phone, "job_title": p.job_title,
                    "linkedin_url": p.linkedin_url, "signal": p.signal,
                    "relevance_score": p.relevance_score,
                    "relevance_reason": p.relevance_reason,
                }
                for p in prospects
            ],
        }


@app.get("/api/sessions/{session_id}/export.csv")
async def export_csv(session_id: int):
    with get_db() as db:
        s = db.get(ProspectSession, session_id)
        if not s:
            raise HTTPException(404, "Not found")
        prospects = db.query(Prospect).filter(Prospect.session_id == session_id).order_by(Prospect.relevance_score.desc()).all()

        buf = io.StringIO()
        fields = ["company", "website", "industry", "employee_count", "description",
                  "country", "first_name", "last_name", "email", "phone",
                  "job_title", "linkedin_url", "signal", "relevance_score"]
        writer = csv.DictWriter(buf, fieldnames=fields)
        writer.writeheader()
        for p in prospects:
            writer.writerow({f: getattr(p, f, "") for f in fields})

    headers = {"Content-Disposition": f'attachment; filename="prospects_{session_id}.csv"'}
    return StreamingResponse(io.BytesIO(buf.getvalue().encode("utf-8")), media_type="text/csv", headers=headers)


if UI_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(UI_DIR)), name="static")
