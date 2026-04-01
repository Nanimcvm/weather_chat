"""
unified_api.py  –  AgriBot unified voice/text API  (port 8011)
Exposes /api/unified_chat — a single-turn conversational endpoint that
always returns a plain-text ``final_answer`` string (no cards or widgets).
"""
#unified_api.py

import asyncio
import uuid
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.config import ALLOWED_ORIGINS
from app.formatters import format_final_answer
from app.intent import extract_intent
from app.location import loc_label, solr_search
from app.pest import fetch_pest
from app.session import SessionState, Step, get_session
from app.state_machine import _missing
from app.validators import extract_date, normalise_date, valid_location_str, valid_sowing_date
from app.weather import fetch_weather

# ── App ───────────────────────────────────────────────────────

app = FastAPI(title="AgriBot – Unified API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*", "X-Session-ID"],
    expose_headers=["X-Session-ID"],
)


# ── Models ────────────────────────────────────────────────────

class UnifiedChatRequest(BaseModel):
    session_id : Optional[str] = None
    message    : str = ""


class UnifiedChatResponse(BaseModel):
    session_id   : str
    status       : str          # "completed" | "incomplete"
    final_answer : Any


# ── Response builder ──────────────────────────────────────────

def _build(s: SessionState, status: str, final_data: Any) -> Dict[str, Any]:
    return {"session_id": s.session_id, "status": status, "final_answer": final_data}


# ══════════════════════════════════════════════════════════════
# INTERNAL: resolve location + fire API
# ══════════════════════════════════════════════════════════════

async def _trigger_api(s: SessionState) -> Dict[str, Any]:
    """Resolve location coords (if needed) and call the appropriate downstream API."""
    if s.resolved_location is None and s.location_str:
        try:
            docs = await solr_search(s.location_str)
        except Exception as e:
            s.step = Step.IDLE
            return _build(s, "incomplete", f"Location search failed: {e}")

        if not docs:
            s.location_str  = None
            s.missing_slots = ["location"]
            s.step          = Step.COLLECTING
            return _build(
                s, "incomplete",
                "I could not find that location. Please state a valid district or state.",
            )

        if len(docs) == 1:
            s.resolved_location = docs[0]
        else:
            # Try exact match before showing a disambiguation list
            clean_query = s.location_str.lower().replace(".", "").replace(",", "").strip()
            match = next(
                (
                    d for d in docs
                    if loc_label(d).lower().replace(".", "").replace(",", "").strip() in clean_query
                ),
                None,
            )
            if match:
                s.resolved_location = match
            else:
                s.location_candidates = docs
                s.step = Step.DISAMBIGUATING
                options = "  ".join(
                    [f"Option {i + 1}: {loc_label(d)}." for i, d in enumerate(docs)]
                )
                return _build(
                    s, "incomplete",
                    f"I found multiple locations. Please choose one by saying its number.  {options}",
                )

    if s.resolved_location is None:
        s.missing_slots = ["location"]
        s.step = Step.COLLECTING
        return _build(s, "incomplete", "Please tell me the location.")

    # Persist confirmed location for future queries in this session
    s.remember_location()

    s.step  = Step.IDLE
    label   = loc_label(s.resolved_location, fallback=s.location_str or "Unknown location")
    sid     = s.session_id

    res = (
        await fetch_pest(s, sid, label)
        if s.intent_type == "pest"
        else await fetch_weather(s, sid, label)
    )

    if res.get("type") == "error":
        return _build(s, "incomplete", res.get("message", "Service error occurred."))

    answer = format_final_answer(s, res.get("data", {}), label)
    return _build(s, "completed", answer)


# ══════════════════════════════════════════════════════════════
# PRIMARY ENDPOINT
# ══════════════════════════════════════════════════════════════

@app.post("/api/unified_chat", response_model=UnifiedChatResponse)
async def unified_chat(
    body: UnifiedChatRequest,
    x_session_id: Optional[str] = Header(default=None, alias="X-Session-ID"),
):
    sid = body.session_id or x_session_id or str(uuid.uuid4())
    s   = get_session(sid)
    msg = body.message.strip()
    s.log("user", msg)

    # ── Disambiguating ────────────────────────────────────────
    if s.step == Step.DISAMBIGUATING:
        cands   = s.location_candidates or []
        m_lower = msg.lower()
        _WORDS  = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5}
        idx     = None

        if msg.isdigit():
            idx = int(msg) - 1
        else:
            for word, num in _WORDS.items():
                if word in m_lower or str(num) in m_lower:
                    idx = num - 1
                    break

        if idx is None:
            clean = m_lower.replace(".", "").replace(",", "").strip()
            for i, c in enumerate(cands):
                if loc_label(c).lower().replace(".", "").replace(",", "").strip() in clean:
                    idx = i
                    break

        if idx is not None and 0 <= idx < len(cands):
            s.resolved_location   = cands[idx]
            s.location_str        = loc_label(s.resolved_location)
            s.location_candidates = None
            s.remember_location()   # ← persist the user's actual choice
            s.missing_slots       = _missing(s)
            if s.missing_slots:
                s.step = Step.COLLECTING
                return _build(
                    s, "incomplete",
                    f"Got it. Please tell me the {' and '.join(s.missing_slots)}.",
                )
            return await _trigger_api(s)

        # Couldn't parse the choice — ask again as a location
        s.missing_slots = ["location"] + [m for m in s.missing_slots if m != "location"]
        s.step = Step.COLLECTING

    # ── Collecting missing slots ──────────────────────────────
    if s.step == Step.COLLECTING:
        sub_intent = await asyncio.to_thread(extract_intent, msg)
        for slot in list(s.missing_slots):
            if slot == "crop":
                raw = sub_intent.get("crop_slug") or msg.lower().replace(" ", "-")
                if len(raw) >= 2:
                    s.crop_slug = raw
                    s.missing_slots = [m for m in s.missing_slots if m != "crop"]
            elif slot == "sowing_date":
                raw_sd = sub_intent.get("sowing_date")
                if raw_sd and valid_sowing_date(str(raw_sd)):
                    s.sowing_date = normalise_date(str(raw_sd))
                    s.missing_slots = [m for m in s.missing_slots if m != "sowing_date"]
                else:
                    extracted = extract_date(msg)
                    if extracted:
                        s.sowing_date = normalise_date(extracted)
                        s.missing_slots = [m for m in s.missing_slots if m != "sowing_date"]
            elif slot == "location":
                raw_loc = sub_intent.get("location") or msg
                if valid_location_str(str(raw_loc)):
                    new_loc = str(raw_loc)
                    # If user typed the same location as remembered, reuse the resolved doc
                    if s.last_location_str and new_loc.lower().strip() == s.last_location_str.lower().strip():
                        s.resolved_location = s.last_resolved_location
                        s.location_str      = s.last_location_str
                    else:
                        # New location — let _trigger_api do the Solr search
                        s.resolved_location = None
                        s.location_str      = new_loc
                    s.missing_slots = [m for m in s.missing_slots if m != "location"]

        s.missing_slots = _missing(s)
        if s.missing_slots:
            return _build(
                s, "incomplete",
                f"Please tell me the {' and '.join(s.missing_slots)}.",
            )
        return await _trigger_api(s)

    # ── Idle – fresh query ────────────────────────────────────
    intent = await asyncio.to_thread(extract_intent, msg)

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

    # Preserve location memory — only reset if user named a new/different location.
    # When no location is mentioned, restore the remembered resolved doc directly
    # so _call_api skips Solr entirely and fires the downstream API immediately.
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
        # No location in this query — restore the remembered resolved doc directly
        s.location_str      = s.last_location_str
        s.resolved_location = s.last_resolved_location

    raw_sd = intent.get("sowing_date")
    s.sowing_date = (
        normalise_date(str(raw_sd))
        if raw_sd and valid_sowing_date(str(raw_sd))
        else None
    )

    s.missing_slots = _missing(s)

    if not s.missing_slots:
        return await _trigger_api(s)

    s.step = Step.COLLECTING
    return _build(
        s, "incomplete",
        f"Please tell me the {' and '.join(s.missing_slots)}.",
    )


# ── Health ────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "api": "unified_chat"}


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("unified_api:app", host="0.0.0.0", port=8011, reload=True)