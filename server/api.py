import asyncio
import csv
import io
import json
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from db.session import init_db, get_db
from db.models import ProspectSession, Prospect
from server import runner
from scraper.maps import search_maps
from scraper.google_email import search_google_emails, search_google_emails_stream

UI_DIR = Path(__file__).resolve().parent.parent / "ui"

app = FastAPI(title="Signal Harvest")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


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



@app.post("/api/maps")
async def maps_search(request: Request):
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
            results = await search_maps(industry, location, limit, log_cb)
            with get_db() as db:
                s = ProspectSession(
                    client_name=client_name or f"{industry} - {location}",
                    mode="maps",
                    input_data=f"{industry} | {location}",
                    status="completed",
                    lead_count=len(results),
                )
                db.add(s)
                db.flush()
                session_id = s.id
                for r in results:
                    row = Prospect(session_id=session_id, **{
                        k: r.get(k, "") for k in [
                            "company","website","industry","employee_count","description",
                            "country","first_name","last_name","email","phone",
                            "job_title","linkedin_url","signal","relevance_reason","source_url"
                        ]
                    })
                    row.relevance_score = r.get("relevance_score", 0.0)
                    db.add(row)
                    await runner.hub.broadcast({"type": "prospect", "prospect": {**r, "id": 0, "session_id": session_id}})

            await runner.hub.broadcast({"type": "done", "session_id": session_id, "status": "completed", "lead_count": len(results)})
        except Exception as e:
            await runner.hub.broadcast({"type": "log", "msg": f"Error: {e}"})
            await runner.hub.broadcast({"type": "done", "session_id": 0, "status": "error", "lead_count": 0})

    asyncio.create_task(_run())
    return {"ok": True}


@app.post("/api/google-email")
async def google_email_search(request: Request):
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
            enrichment = body.get("enrichment", "owner")  # default to owner to save credits
            results = await search_google_emails(industry, location, limit, enrichment, log_cb)
            with get_db() as db:
                s = ProspectSession(
                    client_name=client_name or f"{industry} - {location}",
                    mode="google_email",
                    input_data=f"{industry} | {location}",
                    status="completed",
                    lead_count=len(results),
                )
                db.add(s)
                db.flush()
                session_id = s.id
                for r in results:
                    row = Prospect(session_id=session_id, **{
                        k: r.get(k, "") for k in [
                            "company","website","industry","employee_count","description",
                            "country","first_name","last_name","email","phone",
                            "job_title","linkedin_url","signal","relevance_reason","source_url"
                        ]
                    })
                    row.relevance_score = r.get("relevance_score", 0.0)
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
        # One row per contact for cadence tool import
        fields = ["company", "website", "phone", "industry",
                  "first_name", "last_name", "email", "job_title",
                  "linkedin_url", "email_verified", "score", "signal"]
        writer = csv.DictWriter(buf, fieldnames=fields)
        writer.writeheader()
        for p in prospects:
            base = {
                "company": p.company or "",
                "website": p.website or "",
                "phone": p.phone or "",
                "industry": p.industry or "",
                "score": round(p.relevance_score or 0, 1),
                "signal": p.signal or "",
            }
            # Write primary contact row
            writer.writerow({
                **base,
                "first_name": p.first_name or "",
                "last_name": p.last_name or "",
                "email": p.email or "",
                "job_title": p.job_title or "",
                "linkedin_url": p.linkedin_url or "",
                "email_verified": "yes" if p.signal and "✅" in p.signal else "no",
            })

    headers = {"Content-Disposition": f'attachment; filename="prospects_{session_id}.csv"'}
    return StreamingResponse(io.BytesIO(buf.getvalue().encode("utf-8")), media_type="text/csv", headers=headers)


if UI_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(UI_DIR)), name="static")

class OwnerEmailRequest(BaseModel):
    domain: str
    company: str = ""

@app.post("/api/owner-email")
async def get_owner_email(payload: OwnerEmailRequest):
    """Find owner email for a single domain — used by Prospect Engine Get Email button."""
    try:
        from scraper.owner_finder import find_owner_name
        from scraper.email_verifier import verify_email_smtp
        import re

        domain = payload.domain.replace("https://","").replace("http://","").split("/")[0].strip()
        company = payload.company

        # Try to find owner name
        first, last = await find_owner_name(company, "", "https://"+domain)

        # Generate email permutations
        emails = []
        if first and last:
            f, l = first.lower(), last.lower()
            emails = [
                f"{f}.{l}@{domain}",
                f"{f}@{domain}",
                f"{f[0]}{l}@{domain}",
                f"{l}@{domain}",
            ]
        emails += [f"info@{domain}", f"contact@{domain}", f"owner@{domain}"]

        # Verify each
        for email in emails:
            try:
                valid = await verify_email_smtp(email)
                if valid:
                    return {
                        "email": email,
                        "verified": True,
                        "name": f"{first} {last}".strip() if first else "",
                    }
            except Exception:
                continue

        # Return best guess unverified
        best = emails[0] if emails else None
        return {"email": best, "verified": False, "name": f"{first} {last}".strip() if first else ""}

    except Exception as e:
        return {"email": None, "verified": False, "error": str(e)}

@app.post("/api/verify-email")
async def verify_single_email(payload: dict):
    """Verify a single email address."""
    try:
        from scraper.email_verifier import verify_email_smtp
        email = payload.get("email","")
        valid = await verify_email_smtp(email)
        return {"email": email, "valid": valid}
    except Exception as e:
        return {"email": payload.get("email",""), "valid": False, "error": str(e)}

