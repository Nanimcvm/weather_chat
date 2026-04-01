"""
main.py  –  AgriBot primary API  (port 8010)
Exposes /api/chat (stateful slot-filling chat) and legacy pass-through endpoints.
"""

import asyncio
import re
import uuid
from typing import Dict, Optional

import httpx
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.config import (
    ALLOWED_ORIGINS,
    GFS_HOURLY_URL,
    GFS_INFESTATION_URL,
    GFS_INTERPOLATE_URL,
)
from app.intent import extract_intent
from app.location import loc_label, solr_search
from app.session import SessionState, Step, active_session_count, drop_session, get_session
from app.state_machine import _ask, _call_api, _collect, _fill_slots, _missing, _resolve_and_call
from app.validators import normalise_date, valid_location_str, valid_sowing_date

# ── App ───────────────────────────────────────────────────────

app = FastAPI(title="AgriBot – Primary API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*", "X-Session-ID"],
    expose_headers=["X-Session-ID"],
)


# ── Request model ─────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str = ""
    location_choice_index: Optional[int] = None
    slots: Optional[Dict[str, str]] = None   # set when user submits a form/chip


# ══════════════════════════════════════════════════════════════
# PRIMARY ENDPOINT
# ══════════════════════════════════════════════════════════════

@app.post("/api/chat")
async def chat(
    body: ChatRequest,
    x_session_id: Optional[str] = Header(default=None, alias="X-Session-ID"),
):
    if not x_session_id:
        x_session_id = str(uuid.uuid4())

    s = get_session(x_session_id)
    s.log("user", str(body.slots or body.message))

    print(
        f"[CHAT] sid={x_session_id[:8]} step={s.step} "
        f"missing={s.missing_slots} msg={body.message!r} "
        f"slots={body.slots} choice={body.location_choice_index}"
    )

    # ── Structured slot fill (form/chip submission) ───────────
    if body.slots:
        return await _fill_slots(s, body.slots, x_session_id)

    # ── Disambiguating ────────────────────────────────────────
    if s.step == Step.DISAMBIGUATING:
        cands = s.location_candidates or []

        # Resolve by index (preferred — sent by UI chip click)
        if body.location_choice_index is not None:
            idx = body.location_choice_index
            if 0 <= idx < len(cands):
                s.resolved_location   = cands[idx]
                s.location_str        = loc_label(s.resolved_location)
                s.location_candidates = None
                s.remember_location()   # persist the chosen Solr doc, not the search string
                s.missing_slots       = [m for m in s.missing_slots if m != "location"]
                if s.missing_slots:
                    s.step = Step.COLLECTING
                    return _ask(s, x_session_id)
                return await _call_api(s, x_session_id)

        # Resolve by label — frontend also sends the label text in `message`
        # e.g. {message: "Hyderabad, Telangana", location_choice_index: 0}
        # If message matches one of the candidate labels exactly, use that candidate
        # directly instead of triggering a fresh Solr search.
        if body.message:
            msg_clean = body.message.strip().lower()
            for i, doc in enumerate(cands):
                if loc_label(doc).lower() == msg_clean:
                    s.resolved_location   = doc
                    s.location_str        = loc_label(doc)
                    s.location_candidates = None
                    s.remember_location()   # persist the chosen Solr doc
                    s.missing_slots       = [m for m in s.missing_slots if m != "location"]
                    if s.missing_slots:
                        s.step = Step.COLLECTING
                        return _ask(s, x_session_id)
                    return await _call_api(s, x_session_id)

        # User typed something that doesn't match any candidate — treat as new location input
        s.missing_slots = ["location"] + [m for m in s.missing_slots if m != "location"]
        s.step = Step.COLLECTING
        return await _collect(s, body.message, x_session_id)

    # ── Collecting missing slots ──────────────────────────────
    if s.step == Step.COLLECTING:
        return await _collect(s, body.message, x_session_id)

    # ── Idle – fresh query ────────────────────────────────────
    intent = await asyncio.to_thread(extract_intent, body.message)

    s.intent_type  = "pest" if intent.get("is_pest") else "weather"
    s.crop_slug    = intent.get("crop_slug")
    s.metric       = intent.get("metric") or "ALL"
    s.query_type   = intent.get("query_type") or "single"
    s.day_offset   = intent.get("day_offset")
    s.target_date  = intent.get("target_date")
    s.range_days   = intent.get("range_days")
    s.hour_range   = intent.get("hour_range")
    s.condition    = intent.get("condition")
    s.is_next_week = s.query_type == "range_week" or (s.day_offset or 0) >= 7

    if s.query_type == "single" and s.day_offset is None and not s.target_date:
        s.day_offset = 0

    # Reset per-query state but preserve location memory.
    # Only wipe resolved_location when the user explicitly names a NEW location.
    # When no location is mentioned, keep resolved_location intact so the next
    # call skips Solr entirely and goes straight to the API.
    s.location_candidates = None

    raw_loc = intent.get("location")
    new_location = str(raw_loc) if raw_loc and valid_location_str(str(raw_loc)) else None

    if new_location and new_location != s.last_location_str:
        # User named a different location — start fresh
        s.location_str      = new_location
        s.resolved_location = None
    elif new_location and new_location == s.last_location_str:
        # User named the same location explicitly — keep the resolved doc
        s.location_str      = s.last_location_str
        s.resolved_location = s.last_resolved_location
    else:
        # No location in this query — restore the remembered resolved doc directly.
        # This means _call_api will skip Solr and fire the downstream API immediately.
        s.location_str      = s.last_location_str
        s.resolved_location = s.last_resolved_location

    raw_sd = intent.get("sowing_date")
    s.sowing_date = (
        normalise_date(str(raw_sd))
        if raw_sd and valid_sowing_date(str(raw_sd))
        else None
    )

    s.missing_slots = _missing(s)

    print(
        f"[INTENT] type={s.intent_type} crop={s.crop_slug} "
        f"loc={s.location_str} sd={s.sowing_date} missing={s.missing_slots}"
    )

    if not s.missing_slots:
        # If resolved_location is already set (from memory), skip Solr entirely
        if s.resolved_location:
            return await _call_api(s, x_session_id)
        # location_str is set but not yet resolved — go through Solr
        return await _resolve_and_call(s, x_session_id)

    s.step = Step.COLLECTING
    return _ask(s, x_session_id)


# ══════════════════════════════════════════════════════════════
# DEBUG / UTILITY ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.get("/api/session/{session_id}")
async def inspect_session(session_id: str):
    from app.session import _sessions
    s = _sessions.get(session_id)
    if not s:
        raise HTTPException(404, "Session not found")
    return s.debug()


@app.delete("/api/session/{session_id}")
async def reset_session(session_id: str):
    drop_session(session_id)
    return {"status": "cleared"}


@app.get("/api/debug/location")
async def debug_location(q: str = Query(...)):
    docs = await solr_search(q)
    return {"query": q, "count": len(docs), "docs": docs}


@app.get("/health")
async def health():
    return {"status": "ok", "active_sessions": active_session_count()}


# ══════════════════════════════════════════════════════════════
# LEGACY ENDPOINTS  (old frontend compatibility)
# ══════════════════════════════════════════════════════════════

@app.get("/api/search")
async def legacy_search(q: str = Query(..., min_length=2)):
    intent = await asyncio.to_thread(extract_intent, q)
    raw    = intent.get("location")
    if not raw or not valid_location_str(str(raw)):
        if intent.get("is_pest"):
            return {
                "status":        "need_info",
                "missing_slots": ["location"],
                "intent":        intent,
                "response":      {"docs": [], "numFound": 0, "start": 0},
            }
        raise HTTPException(400, "Could not extract a location.")
    docs = await solr_search(str(raw))
    return {"response": {"docs": docs, "numFound": len(docs)}, "intent": intent}


class InfestationRequest(BaseModel):
    sowing_date : Optional[str] = "10-01-2026"
    crop_slug   : Optional[str] = "paddy"
    state_name  : Optional[str] = "Odisha"


@app.post("/api/pest/infestation")
async def legacy_pest(
    body: InfestationRequest,
    lat: float = Query(...),
    lon: float = Query(...),
    is_next_week: bool = Query(False),
):
    sd = body.sowing_date
    if sd and re.match(r"^\d{4}-\d{2}-\d{2}$", sd):
        y, m, d = sd.split("-")
        sd = f"{d}-{m}-{y}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(
            GFS_INFESTATION_URL,
            params={"lat": lat, "lon": lon, "is_next_week": str(is_next_week).lower()},
            json={"sowing_date": sd, "crop_slug": body.crop_slug, "state_name": body.state_name},
        )
        r.raise_for_status()
        return r.json()


@app.get("/api/weather/daily")
async def legacy_daily(lat: float, lon: float):
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(GFS_INTERPOLATE_URL, params={"lat": lat, "lon": lon})
        r.raise_for_status()
        return r.json()


@app.get("/api/weather/hourly")
async def legacy_hourly(
    lat: float,
    lon: float,
    hours: int = Query(default=24, ge=1, le=240),
):
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(GFS_HOURLY_URL, params={"lat": lat, "lon": lon})
        r.raise_for_status()
        return r.json()


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8010, reload=True)