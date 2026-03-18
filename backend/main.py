from fastapi import FastAPI, HTTPException, Query, Header
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
import asyncio
import time as time_module
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
from enum import Enum
from pydantic import BaseModel
from groq import Groq
from dotenv import load_dotenv
import re

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
groq_client = Groq(api_key=GROQ_API_KEY)

os.environ["LANGCHAIN_TRACING_V2"] = os.getenv("LANGCHAIN_TRACING_V2", "true")
os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGSMITH_API_KEY")
os.environ["LANGCHAIN_PROJECT"] = os.getenv("LANGSMITH_PROJECT", "agribot")
os.environ["LANGCHAIN_ENDPOINT"] = os.getenv("LANGCHAIN_ENDPOINT", "https://api.smith.langchain.com")

from langsmith import traceable

SOLR_SEARCH_URL     = os.getenv("SOLR_SEARCH_URL")
SOLR_AUTH           = os.getenv("SOLR_AUTH_TOKEN")
GFS_INTERPOLATE_URL = os.getenv("GFS_INTERPOLATE_URL")
GFS_HOURLY_URL      = os.getenv("GFS_HOURLY_URL")
GFS_INFESTATION_URL = os.getenv("GFS_INFESTATION_URL")
ALLOWED_ORIGINS     = os.getenv("ALLOWED_ORIGINS", "*").split(",")

SESSION_TTL = 1800  # 30 min inactivity


class Step(str, Enum):
    IDLE            = "idle"
    COLLECTING      = "collecting"       # asking for missing slots
    DISAMBIGUATING  = "disambiguating"   # multiple Solr results, pick one


class SessionState:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.step       = Step.IDLE

        # ── Set by LLM intent on first message ──────────────────
        self.intent_type  = None    # "weather" | "pest"
        self.crop_slug    = None
        self.metric       = "ALL"
        self.query_type   = "single"
        self.day_offset   = None
        self.target_date  = None
        self.range_days   = None
        self.hour_range   = None
        self.condition    = None
        self.is_next_week = False

        # ── Slots (filled progressively) ────────────────────────
        self.location_str        = None   # raw string from user
        self.resolved_location   = None   # Solr doc with coords
        self.location_candidates = None   # list when ambiguous
        self.sowing_date         = None   # normalised DD-MM-YYYY

        # ── Which slots still need answers ──────────────────────
        self.missing_slots: List[str] = []

        self.history: List[Dict] = []
        self.created_at  = time_module.time()
        self.last_active = time_module.time()

    def touch(self):
        self.last_active = time_module.time()

    def log(self, role: str, content: str):
        self.history.append({"role": role, "content": content, "ts": time_module.time()})
        if len(self.history) > 20:
            self.history = self.history[-20:]

    def debug(self):
        return {
            "session_id":        self.session_id,
            "step":              self.step,
            "intent_type":       self.intent_type,
            "crop_slug":         self.crop_slug,
            "location_str":      self.location_str,
            "resolved_location": bool(self.resolved_location),
            "sowing_date":       self.sowing_date,
            "missing_slots":     self.missing_slots,
        }


_sessions: Dict[str, SessionState] = {}


def get_session(sid: str) -> SessionState:
    now = time_module.time()
    stale = [k for k, v in _sessions.items() if now - v.last_active > SESSION_TTL]
    for k in stale:
        del _sessions[k]
    if sid not in _sessions:
        _sessions[sid] = SessionState(sid)
    s = _sessions[sid]
    s.touch()
    return s


# ══════════════════════════════════════════════════════════════
# INTENT EXTRACTION  (LLM, first message only)
# ══════════════════════════════════════════════════════════════

_intent_cache: Dict[str, Dict] = {}

INTENT_PROMPT = """
You are an intent extraction engine for an agriculture chatbot.
Extract slots from the user query and return STRICT JSON only.

{
  "intent": "weather | pest",
  "is_pest": boolean,
  "location": string | null,
  "crop_slug": string | null,
  "sowing_date": string | null,
  "metric": "ALL | Tmax | Tmin | Tavg | Rainfall | RH | Wind_Speed",
  "query_type": "single | hourly | range | range_week | conditional_rain | conditional_condition | pest_forecast",
  "day_offset": number | null,
  "target_date": "YYYY-MM-DD" | null,
  "range_days": number | null,
  "hour_range": number | null,
  "condition": string | null
}

Rules:
- pest queries (pests/insects/infestation/disease/attack): intent=pest, is_pest=true
- weather queries (rain/temp/humidity/wind/forecast): intent=weather, is_pest=false
- NO date/time mentioned → day_offset=0, query_type=single  ← ADD THIS
- "today", "now", "current", "right now" → day_offset=0
- "tomorrow" → day_offset=1
- "this week", "next 7 days", "weekly" → query_type=range_week
- "next few days", "forecast" → query_type=range, range_days=5
- calendar dates → target_date YYYY-MM-DD, day_offset=null
- sowing dates → DD-MM-YYYY format
- crop name → crop_slug lowercase hyphenated e.g. "paddy", "sugar-cane"
- temperature/temp → metric=Tavg (unless max/min specified)
- max temp / high → metric=Tmax
- min temp / low / cold → metric=Tmin
- rain/rainfall → metric=Rainfall
- humidity → metric=RH
- wind → metric=Wind_Speed
- multiple metrics or general weather → metric=ALL

Return JSON only. No explanation.
"""


@traceable(name="intent_extraction")
def extract_intent(query: str) -> Dict:
    if query in _intent_cache:
        return _intent_cache[query]
    try:
        r = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            temperature=0, max_tokens=200,
            messages=[
                {"role": "system", "content": INTENT_PROMPT},
                {"role": "user",   "content": query},
            ],
            response_format={"type": "json_object"},
            timeout=10,
        )
        intent = json.loads(r.choices[0].message.content)
        _intent_cache[query] = intent
        return intent
    except Exception:
        return {
            "intent": "weather", "is_pest": False,
            "location": None, "crop_slug": None, "sowing_date": None,
            "metric": "ALL", "query_type": "single",
            "day_offset": 0, "target_date": None,
            "range_days": None, "hour_range": None, "condition": None,
        }


# ══════════════════════════════════════════════════════════════
# SOLR  (location search + coord resolution)
# ══════════════════════════════════════════════════════════════

_cache:    Dict[str, Any]   = {}
_cache_ts: Dict[str, float] = {}
CACHE_TTL = 300


def _cget(k):
    if k in _cache and time_module.time() - _cache_ts[k] < CACHE_TTL:
        return _cache[k]
    return None


def _cset(k, v):
    _cache[k] = v
    _cache_ts[k] = time_module.time()


COORD_PRIORITY = [
    ("village_latitude", "village_longitude"),
    ("village_lat", "village_lon"),
    ("mandal_latitude", "mandal_longitude"),
    ("taluk_latitude", "taluk_longitude"),
    ("subdistrict_latitude", "subdistrict_longitude"),
    ("block_latitude", "block_longitude"),
    ("district_latitude", "district_longitude"),
    ("district_lat", "district_lon"),
    ("state_latitude", "state_longitude"),
    ("state_lat", "state_lon"),
]


def best_coords(doc: dict):
    for lf, lo in COORD_PRIORITY:
        lat = doc.get(lf)
        lon = doc.get(lo)
        if lat and lon:
            lat = lat[0] if isinstance(lat, list) else lat
            lon = lon[0] if isinstance(lon, list) else lon
            if lat and lon:
                return lat, lon
    return None, None


async def solr_search(q: str) -> List[Dict]:
    key = f"solr:{q.lower().strip()}"
    hit = _cget(key)
    if hit is not None:
        return hit
    solr_q = (
        f'(village:"{q}" OR state:"{q}" OR district:"{q}")'
        f' OR (village:{q}~1 OR state:{q}~1 OR district:{q}~1)'
    )
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(
            SOLR_SEARCH_URL,
            params={"q": solr_q, "rows": 8, "wt": "json"},
            headers={"Authorization": SOLR_AUTH},
        )
        r.raise_for_status()
        docs = r.json().get("response", {}).get("docs", [])
        for d in docs:
            d["_best_lat"], d["_best_lon"] = best_coords(d)
        _cset(key, docs)
        return docs


def _unwrap(val):
    """Unwrap Solr array fields to a plain string."""
    if isinstance(val, list):
        return val[0] if val else None
    return val


# FIX - accept a fallback parameter
def loc_label(doc: dict, fallback: str = "Unknown location") -> str:
    if doc is None:
        return fallback
    name = _unwrap(doc.get("village")) or _unwrap(doc.get("district")) or _unwrap(doc.get("state"))
    state = _unwrap(doc.get("state"))
    parts = [name, state] if name != state else [name]
    return ", ".join(p for p in parts if p)


# ══════════════════════════════════════════════════════════════
# SLOT VALIDATORS
# ══════════════════════════════════════════════════════════════

def valid_location_str(s: str) -> bool:
    return bool(s) and len(s.strip()) >= 2 and not re.fullmatch(r"[\W\d\s]+", s.strip())


def valid_sowing_date(s: str) -> bool:
    return bool(
        re.match(r"^\d{2}-\d{2}-\d{4}$", s)
        or re.match(r"^\d{4}-\d{2}-\d{2}$", s)
    )


def normalise_date(s: str) -> str:
    """YYYY-MM-DD → DD-MM-YYYY.  DD-MM-YYYY unchanged."""
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        y, m, d = s.split("-")
        return f"{d}-{m}-{y}"
    return s


def extract_date(text: str) -> Optional[str]:
    """Pull a date out of free text.  Returns DD-MM-YYYY or None."""
    # DD-MM-YYYY or DD/MM/YYYY
    m = re.search(r"\b(\d{1,2})[-/](\d{1,2})[-/](\d{4})\b", text)
    if m:
        return f"{m.group(1).zfill(2)}-{m.group(2).zfill(2)}-{m.group(3)}"
    # YYYY-MM-DD
    m = re.search(r"\b(\d{4})[-/](\d{2})[-/](\d{2})\b", text)
    if m:
        y, mo, d = m.group(1), m.group(2), m.group(3)
        return f"{d}-{mo}-{y}"
    # "10 december 2025" / "december 10 2025"
    MONTHS = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        "january": 1, "february": 2, "march": 3, "april": 4,
        "june": 6, "july": 7, "august": 8, "september": 9,
        "october": 10, "november": 11, "december": 12,
    }
    m = re.search(r"\b(\d{1,2})\s+([a-zA-Z]+)\s+(\d{4})\b", text)
    if m and m.group(2).lower() in MONTHS:
        return f"{m.group(1).zfill(2)}-{str(MONTHS[m.group(2).lower()]).zfill(2)}-{m.group(3)}"
    m = re.search(r"\b([a-zA-Z]+)\s+(\d{1,2})\s+(\d{4})\b", text)
    if m and m.group(1).lower() in MONTHS:
        return f"{m.group(2).zfill(2)}-{str(MONTHS[m.group(1).lower()]).zfill(2)}-{m.group(3)}"
    return None

async def _fill_slots(s: SessionState, slots: Dict[str, str], sid: str) -> Dict:
    """
    Fill multiple slots at once from a structured dict.
    Validates each value. Returns disambiguation or calls API when done.
    """
    errors: Dict[str, str] = {}

    # Fill crop
    if "crop" in slots:
        crop_val = slots["crop"].strip().lower().replace(" ", "-")
        if len(crop_val) >= 2:
            s.crop_slug = crop_val
            s.missing_slots = [m for m in s.missing_slots if m != "crop"]
        else:
            errors["crop"] = "Please enter a valid crop name (e.g. paddy, wheat, cotton)."

    # Fill sowing_date
    if "sowing_date" in slots:
        extracted = extract_date(slots["sowing_date"])
        if not extracted:
            errors["sowing_date"] = (
                "Please enter a valid sowing date in DD-MM-YYYY format (e.g. 10-12-2025)."
            )
        else:
            s.sowing_date = normalise_date(extracted)
            s.missing_slots = [m for m in s.missing_slots if m != "sowing_date"]

    # Fill location (do this last — may trigger disambiguation)
    if "location" in slots:
        candidate = slots["location"].strip()
        if not valid_location_str(candidate):
            errors["location"] = "Please enter a valid location name (district or state)."
        else:
            try:
                docs = await solr_search(candidate)
            except Exception as e:
                errors["location"] = f"Location lookup failed: {e}"
                docs = []

            if not docs:
                errors["location"] = (
                    f"<strong>{candidate}</strong> wasn't found. "
                    "Please enter a valid district or state name."
                )
            elif len(docs) == 1:
                s.resolved_location = docs[0]
                s.location_str      = loc_label(s.resolved_location)
                s.missing_slots     = [m for m in s.missing_slots if m != "location"]
            else:
                # Ambiguous — but crop/sowing already saved above ✅
                s.location_candidates = docs
                s.location_str        = candidate
                s.missing_slots       = [m for m in s.missing_slots if m != "location"]
                s.step = Step.DISAMBIGUATING
                return {
                    "type":       "ask_location_choice",
                    "session_id": sid,
                    "candidates": [
                        {"index": i, "label": loc_label(d)}
                        for i, d in enumerate(docs)
                    ],
                }

    # Recompute missing
    s.missing_slots = _missing(s)

    if errors or s.missing_slots:
        s.step = Step.COLLECTING
        return _ask(s, sid, errors if errors else None)

    return await _call_api(s, sid)

# ══════════════════════════════════════════════════════════════
# FASTAPI APP
# ══════════════════════════════════════════════════════════════

app = FastAPI(title="AgriBot Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*", "X-Session-ID"],
    expose_headers=["X-Session-ID"],
)


class ChatRequest(BaseModel):
    message: str = ""
    location_choice_index: Optional[int] = None
    slots: Optional[Dict[str, str]] = None  # set when user picks a chip


@app.post("/api/chat")
async def chat(
    body: ChatRequest,
    x_session_id: Optional[str] = Header(default=None, alias="X-Session-ID"),
):
    if not x_session_id:
        x_session_id = str(uuid.uuid4())

    s = get_session(x_session_id)
    s.log("user", str(body.slots or body.message))

    print(f"[CHAT] sid={x_session_id[:8]} step={s.step} "
          f"missing={s.missing_slots} msg={body.message!r} "
          f"slots={body.slots} choice={body.location_choice_index}")

    # ── STRUCTURED SLOT FILL ──────────────────────────────────────
    if body.slots:
        return await _fill_slots(s, body.slots, x_session_id)

    # ── DISAMBIGUATING ────────────────────────────────────────────
    if s.step == Step.DISAMBIGUATING:
        if body.location_choice_index is not None:
            cands = s.location_candidates or []
            if 0 <= body.location_choice_index < len(cands):
                s.resolved_location   = cands[body.location_choice_index]
                s.location_str        = loc_label(s.resolved_location)
                s.location_candidates = None
                s.missing_slots       = [m for m in s.missing_slots if m != "location"]
                if s.missing_slots:
                    s.step = Step.COLLECTING
                    return _ask(s, x_session_id)
                return await _call_api(s, x_session_id)
        # User typed instead of clicking — treat as new location
        s.missing_slots = ["location"] + [m for m in s.missing_slots if m != "location"]
        s.step = Step.COLLECTING
        return await _collect(s, body.message, x_session_id)

    # ── COLLECTING ────────────────────────────────────────────────
    if s.step == Step.COLLECTING:
        return await _collect(s, body.message, x_session_id)

    # ── IDLE  (fresh query) ───────────────────────────────────────
    intent = await asyncio.to_thread(extract_intent, body.message)

    # Load everything from LLM into session
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

    if (s.query_type == "single" and s.day_offset == None and not s.target_date) :
        s.day_offset = 0 
    # Reset location / sowing for this new query
    s.resolved_location   = None
    s.location_candidates = None

    # Seed from LLM if already present in the query
    raw_loc = intent.get("location")
    s.location_str = str(raw_loc) if raw_loc and valid_location_str(str(raw_loc)) else None

    raw_sd = intent.get("sowing_date")
    s.sowing_date = normalise_date(str(raw_sd)) if raw_sd and valid_sowing_date(str(raw_sd)) else None

    # Compute what's still missing
    s.missing_slots = _missing(s)

    print(f"[INTENT] type={s.intent_type} crop={s.crop_slug} "
          f"loc={s.location_str} sd={s.sowing_date} missing={s.missing_slots}")

    if not s.missing_slots:
        # Location string is known — resolve coords and fire
        return await _resolve_and_call(s, x_session_id)

    s.step = Step.COLLECTING
    return _ask(s, x_session_id)


# ══════════════════════════════════════════════════════════════
# STATE-MACHINE HELPERS
# ══════════════════════════════════════════════════════════════

def _missing(s: SessionState) -> List[str]:
    out = []
    if not s.location_str:
        out.append("location")
    if s.intent_type == "pest" and not s.crop_slug:
        out.append("crop")
    if s.intent_type == "pest" and not s.sowing_date:
        out.append("sowing_date")
    return out


def _ask(s: SessionState, sid: str, errors: Dict[str, str] = None) -> Dict:
    """
    Build a response card asking for all currently missing slots at once.
    errors = {"location": "...", "sowing_date": "..."} for inline field errors.
    """
    FIELD_META = {
        "location":    {"label": "Location",    "hint": "e.g. Hyderabad, Guntur, Vijayawada"},
        "crop":        {"label": "Crop",        "hint": "e.g. paddy, potato, blackgram"},
        "sowing_date": {"label": "Sowing Date", "hint": "DD-MM-YYYY  e.g. 10-12-2025"},
    }
    fields = []
    for slot in s.missing_slots:
        f = {"slot": slot, **FIELD_META.get(slot, {"label": slot, "hint": ""})}
        if errors and slot in errors:
            f["error"] = errors[slot]
        fields.append(f)

    return {
        "type":        "ask_slots",
        "session_id":  sid,
        "fields":      fields,
        "intent_type": s.intent_type,
        "crop_slug":   s.crop_slug,
    }


async def _collect(s: SessionState, message: str, sid: str) -> Dict:
    """
    User answered while we were COLLECTING.
    Parse their message to fill each still-missing slot.
    Re-ask any slot whose answer was invalid or not found.
    """
    errors: Dict[str, str] = {}

    # Pass 1: fill non-location slots first so they are saved before disambiguation
    for slot in list(s.missing_slots):
        if slot == "crop":
            crop_val = message.strip().lower().replace(" ", "-")
            if len(crop_val) >= 2:
                s.crop_slug = crop_val
                s.missing_slots = [m for m in s.missing_slots if m != "crop"]
            else:
                errors["crop"] = "Please enter a valid crop name (e.g. paddy, wheat, cotton)."

        elif slot == "sowing_date":
            extracted = extract_date(message)
            if not extracted:
                errors["sowing_date"] = (
                    "Please enter a valid sowing date in DD-MM-YYYY format "
                    "(e.g. 10-12-2025)."
                )
            else:
                s.sowing_date   = normalise_date(extracted)
                s.missing_slots = [m for m in s.missing_slots if m != "sowing_date"]

    # Pass 2: resolve location (may return early for disambiguation)
    if "location" in s.missing_slots:
        candidate = message.strip()
        if not valid_location_str(candidate):
            errors["location"] = "Please enter a valid location name (district or state)."
        else:
            try:
                docs = await solr_search(candidate)
            except Exception as e:
                errors["location"] = f"Location lookup failed: {e}"
                docs = []

            if not docs:
                errors["location"] = (
                    f"<strong>{candidate}</strong> wasn't found. "
                    "Please enter a valid district or state name."
                )
            elif len(docs) == 1:
                s.resolved_location = docs[0]
                s.location_str      = loc_label(s.resolved_location)
                s.missing_slots     = [m for m in s.missing_slots if m != "location"]
            else:
                # Ambiguous — sowing_date already saved above
                s.location_candidates = docs
                s.location_str        = candidate
                s.missing_slots       = [m for m in s.missing_slots if m != "location"]
                s.step = Step.DISAMBIGUATING
                return {
                    "type":       "ask_location_choice",
                    "session_id": sid,
                    "candidates": [
                        {"index": i, "label": loc_label(d)}
                        for i, d in enumerate(docs)
                    ],
                }

    # Recompute (in case we missed something)
    s.missing_slots = _missing(s)

    if s.missing_slots or errors:
        s.step = Step.COLLECTING
        return _ask(s, sid, errors if errors else None)

    # All done
    return await _call_api(s, sid)


async def _resolve_and_call(s: SessionState, sid: str) -> Dict:
    """
    location_str is set but coords not yet resolved.
    Search Solr, handle ambiguity, then call API.
    """
    try:
        docs = await solr_search(s.location_str)
    except Exception as e:
        s.step = Step.IDLE
        return _err(f"Location search failed: {e}", sid)

    if not docs:
        s.location_str  = None
        s.missing_slots = _missing(s)
        s.step = Step.COLLECTING
        return _ask(
            s, sid,
            errors={"location": f"<strong>{s.location_str or 'That location'}</strong> wasn't found."}
        )

    if len(docs) == 1:
        s.resolved_location = docs[0]
        return await _call_api(s, sid)

    s.location_candidates = docs
    s.step = Step.DISAMBIGUATING
    return {
        "type":       "ask_location_choice",
        "session_id": sid,
        "candidates": [{"index": i, "label": loc_label(d)} for i, d in enumerate(docs)],
    }


# ══════════════════════════════════════════════════════════════
# API CALLERS
# ══════════════════════════════════════════════════════════════

async def _call_api(s: SessionState, sid: str) -> Dict:
    # If location string is known but Solr doc not yet fetched, resolve now.
    # This happens when the LLM extracted the location on the first message
    # but we went straight to COLLECTING for other missing slots.
    if s.resolved_location is None and s.location_str:
        try:
            docs = await solr_search(s.location_str)
        except Exception as e:
            s.step = Step.IDLE
            return _err(f"Location search failed: {e}", sid)

        if not docs:
            s.location_str  = None
            s.missing_slots = ["location"]
            s.step = Step.COLLECTING
            return _ask(s, sid, errors={"location": f"Could not find the location. Please enter a valid district or state."})

        if len(docs) == 1:
            s.resolved_location = docs[0]
        else:
            s.location_candidates = docs
            s.missing_slots       = ["location"]
            s.step = Step.DISAMBIGUATING
            return {
                "type":       "ask_location_choice",
                "session_id": sid,
                "candidates": [{"index": i, "label": loc_label(d)} for i, d in enumerate(docs)],
            }

    if s.resolved_location is None:
        s.missing_slots = ["location"]
        s.step = Step.COLLECTING
        return _ask(s, sid)

    s.step = Step.IDLE   # ready for next query
    label = loc_label(s.resolved_location, fallback=s.location_str or "Unknown location")
    if s.intent_type == "pest":
        return await _pest(s, sid, label)
    return await _weather(s, sid, label)


async def _weather(s: SessionState, sid: str, label: str) -> Dict:
    lat = s.resolved_location.get("_best_lat")
    lon = s.resolved_location.get("_best_lon")
    if not lat or not lon:
        return _err("Could not resolve coordinates.", sid)

    try:
        # Hourly?
        hr = 0
        try:
            hr = int(s.hour_range or 0)
        except (TypeError, ValueError):
            pass
        is_hourly = hr > 0 or s.query_type == "hourly"

        async with httpx.AsyncClient(timeout=15.0) as client:
            if is_hourly:
                r = await client.get(GFS_HOURLY_URL, params={"lat": lat, "lon": lon})
                r.raise_for_status()
                data = r.json()
                ist_now = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).replace(tzinfo=None)

                def fh(records):
                    out = []
                    for rec in records:
                        try:
                            if datetime.fromisoformat(rec.get("Date_time","")) >= ist_now:
                                out.append(rec)
                        except Exception:
                            out.append(rec)
                    return out[:hr or 24]

                if isinstance(data, dict):
                    fk = next((k for k in data if isinstance(data[k], list)), None)
                    if fk:
                        data[fk] = fh(data[fk])
                else:
                    data = fh(data)
                return {"type": "weather_result", "session_id": sid,
                        "location_label": label, "intent": _intent_dict(s),
                        "is_hourly": True, "data": data}

            # Daily
            from datetime import date as dc
            ist_today = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).date()
            RANGE_T   = {"range","range_few","range_week","conditional_rain","conditional_condition"}

            day_offset = None
            if s.day_offset is not None:
                try:
                    day_offset = int(s.day_offset)
                except (TypeError, ValueError):
                    pass

            res_date   = None
            res_offset = None
            if s.query_type not in RANGE_T:
                if s.target_date and str(s.target_date) not in ("null",""):
                    res_date   = dc.fromisoformat(str(s.target_date))
                    res_offset = (res_date - ist_today).days
                    day_offset = res_offset
                elif day_offset is not None:
                    res_offset = day_offset
                    res_date   = ist_today + timedelta(days=day_offset)

            r = await client.get(GFS_INTERPOLATE_URL, params={"lat": lat, "lon": lon})
            r.raise_for_status()
            data = r.json()

            def from_today(recs):
                return [rec for rec in recs if _rec_date(rec) >= ist_today]

            def apply(recs):
                qt = s.query_type
                if qt in ("range","range_few","range_week"):
                    return from_today(recs)[:(s.range_days or 7)], None
                if qt == "conditional_rain":
                    fut   = from_today(recs)[:(s.range_days or len(recs))]
                    rainy = [rec for rec in fut if (rec.get("Rainfall") or 0) > 0]
                    return (rainy, None) if rainy else ([], "☀️ No rainfall expected.")
                if qt == "conditional_condition":
                    COND = {
                        "hot":   lambda r: r.get("Tmax", 0) > 38,
                        "warm":  lambda r: r.get("Tmax", 0) > 30,
                        "cold":  lambda r: r.get("Tmin", 99) < 15,
                        "windy": lambda r: r.get("Wind_Speed", 0) > 20,
                        "humid": lambda r: r.get("RH", 0) > 80,
                    }
                    fn = COND.get(s.condition or "", lambda r: True)
                    matched = [rec for rec in from_today(recs) if fn(rec)]
                    return (matched, None) if matched else ([], f"No '{s.condition}' conditions expected.")
                if res_date is not None:
                    if (res_offset or 0) < 0:
                        return [], "GFS only provides forecasts from today onwards."
                    return [rec for rec in recs if _rec_date(rec) == res_date], None
                return from_today(recs), None

            msg = None
            if isinstance(data, dict):
                fk = next((k for k in data if isinstance(data[k], list)), None)
                if fk:
                    data[fk], msg = apply(data[fk])
            else:
                data, msg = apply(data)
                data = {"Forecast data": data}
            if msg:
                data["_message"] = msg

            return {"type": "weather_result", "session_id": sid,
                    "location_label": label, "intent": _intent_dict(s),
                    "is_hourly": False, "data": data}

    except httpx.TimeoutException:
        return _err("Weather service timed out.", sid)
    except Exception as e:
        return _err(f"Weather fetch failed: {e}", sid)


async def _pest(s: SessionState, sid: str, label: str) -> Dict:
    lat = s.resolved_location.get("_best_lat")
    lon = s.resolved_location.get("_best_lon")
    if not lat or not lon:
        return _err("Could not resolve coordinates.", sid)

    state = s.resolved_location.get("state")
    if isinstance(state, list):
        state = state[0]

    sowing = normalise_date(s.sowing_date)
    crop   = s.crop_slug 
    key    = f"pest:{lat}:{lon}:{s.is_next_week}:{sowing}:{crop}:{state}"

    if (cached := _cget(key)):
        return {"type": "pest_result", "session_id": sid,
                "location_label": label, "state": state,
                "is_next_week": s.is_next_week,
                "intent": _intent_dict(s), "data": cached}

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                GFS_INFESTATION_URL,
                params={"lat": lat, "lon": lon, "is_next_week": str(s.is_next_week).lower()},
                json={"sowing_date": sowing, "crop_slug": crop, "state_name": state},
            )
            r.raise_for_status()
            data = r.json()
        _cset(key, data)
        return {"type": "pest_result", "session_id": sid,
                "location_label": label, "state": state,
                "is_next_week": s.is_next_week,
                "intent": _intent_dict(s), "data": data}
    except httpx.TimeoutException:
        return _err("Pest service timed out.", sid)
    except Exception as e:
        return _err(f"Pest fetch failed: {e}", sid)


def _intent_dict(s: SessionState) -> Dict:
    return {
        "intent":      s.intent_type,
        "is_pest":     s.intent_type == "pest",
        "metric":      s.metric,
        "query_type":  s.query_type,
        "day_offset":  s.day_offset,
        "target_date": s.target_date,
        "range_days":  s.range_days,
        "hour_range":  s.hour_range,
        "condition":   s.condition,
        "crop_slug":   s.crop_slug,
        "sowing_date": s.sowing_date,
    }


def _err(message: str, sid: str) -> Dict:
    return {"type": "error", "session_id": sid, "message": message}


def _rec_date(r: dict):
    from datetime import date
    try:
        return datetime.fromisoformat(r.get("Date_time", r.get("Date", ""))).date()
    except Exception:
        return datetime.min.date()


# ══════════════════════════════════════════════════════════════
# DEBUG / UTILITY ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.get("/api/session/{session_id}")
async def inspect_session(session_id: str):
    s = _sessions.get(session_id)
    if not s:
        raise HTTPException(404, "Session not found")
    return s.debug()


@app.delete("/api/session/{session_id}")
async def reset_session_ep(session_id: str):
    _sessions.pop(session_id, None)
    return {"status": "cleared"}


@app.get("/api/debug/location")
async def debug_location(q: str = Query(...)):
    docs = await solr_search(q)
    return {"query": q, "count": len(docs), "docs": docs}


@app.get("/health")
async def health():
    return {"status": "ok", "active_sessions": len(_sessions)}


# ══════════════════════════════════════════════════════════════
# LEGACY ENDPOINTS (old frontend still works)
# ══════════════════════════════════════════════════════════════

@app.get("/api/search")
async def legacy_search(q: str = Query(..., min_length=2)):
    intent = await asyncio.to_thread(extract_intent, q)
    raw    = intent.get("location")
    if not raw or not valid_location_str(str(raw)):
        if intent.get("is_pest"):
            return {"status": "need_info", "missing_slots": ["location"], "intent": intent,
                    "response": {"docs": [], "numFound": 0, "start": 0}}
        raise HTTPException(400, "Could not extract a location.")
    docs = await solr_search(str(raw))
    return {"response": {"docs": docs, "numFound": len(docs)}, "intent": intent}


class InfestationRequest(BaseModel):
    sowing_date: Optional[str] = "10-01-2026"
    crop_slug:   Optional[str] = "paddy"
    state_name:  Optional[str] = "Odisha"


@app.post("/api/pest/infestation")
async def legacy_pest(
    body: InfestationRequest,
    lat: float = Query(...), lon: float = Query(...),
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
async def legacy_hourly(lat: float, lon: float, hours: int = Query(default=24, ge=1, le=240)):
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(GFS_HOURLY_URL, params={"lat": lat, "lon": lon})
        r.raise_for_status()
        return r.json()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8010)