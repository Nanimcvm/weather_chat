from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
import asyncio
import time as time_module
import json
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict
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

SOLR_SEARCH_URL = os.getenv("SOLR_SEARCH_URL")
SOLR_AUTH = os.getenv("SOLR_AUTH_TOKEN")

GFS_INTERPOLATE_URL = os.getenv("GFS_INTERPOLATE_URL")
GFS_HOURLY_URL = os.getenv("GFS_HOURLY_URL")
GFS_INFESTATION_URL = os.getenv("GFS_INFESTATION_URL")

intent_cache: Dict[str, Dict] = {}

app = FastAPI(title="WeatherSnap ChatBot Backend")

INTENT_SYSTEM_PROMPT = """
You are an intent extraction engine for an agriculture chatbot.

You must classify the query and extract slots.

Return STRICT JSON:

{
 "intent": "weather | pest",
 "location": string | null,
 "metric": string | null,
 "query_type": "single | hourly | range | conditional_rain | conditional_condition | pest_forecast",
 "day_offset": number | null,
 "range_days": number | null,
 "hour_range": number | null,
 "condition": string | null,
 "is_pest": boolean,
 "crop_slug": string | null,
 "sowing_date": string | null,
 "missing_slots": list
}

Rules:

PEST QUERIES:
If user asks about pests, insects, infestation, diseases, or attack:
- intent = pest
- is_pest = true
- query_type = pest_forecast

WEATHER QUERIES:
If user asks about rain, temperature, humidity, wind, or forecast:
- intent = weather
- is_pest = false

Date rules:
- "today" -> day_offset=0
- "tomorrow" -> day_offset=1

If the user specifies a calendar date such as:
- "23rd march 2026"
- "march 23 2026"
- "23/03/2026"

Then:
- convert it to ISO format YYYY-MM-DD
- store it in target_date
- day_offset = null

Output:

{
 "location": "Vijayawada",
 "metric": "Tavg",
 "query_type": "single",
 "target_date": "2026-03-23",
 "day_offset": null
}

Example:

User:
what will be temperature in vijayawada on 23rd march 2026
Crop rules:
- extract crop name if mentioned.

Sowing date rules:
Convert dates to DD-MM-YYYY.

Example:

User:
which pests will attack paddy if sown on 10 december 2025 in hyderabad

Output:

{
 "intent": "weather | pest",
 "location": string | null,
 "metric": string | null,
 "query_type": "single | hourly | range | conditional_rain | conditional_condition | pest_forecast",
 "day_offset": number | null,
 "target_date": string | null,
 "range_days": number | null,
 "hour_range": number | null,
 "condition": string | null,
 "is_pest": boolean,
 "crop_slug": string | null,
 "sowing_date": string | null,
 "missing_slots": list
}

Return JSON only.
"""

# Production: restrict to your frontend origin
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

@traceable(name="intent_extraction")
def extract_intent(query: str) -> Dict:

    if query in intent_cache:
        return intent_cache[query]

    try:

        completion = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            temperature=0,
            max_tokens=200,
            messages=[
                {"role": "system", "content": INTENT_SYSTEM_PROMPT},
                {"role": "user", "content": query}
            ],
            response_format={"type": "json_object"},
            timeout=10
        )

        content = completion.choices[0].message.content
        intent = json.loads(content)
        
        intent_cache[query] = intent

        return intent

    except Exception:

        return {
            "location": query,
            "metric": "ALL",
            "query_type": "single",
            "day_offset": 0,
            "target_date": None,
            "range_days": None,
            "hour_range": None,
            "condition": None,
            "is_pest": False,
            "crop_slug": None,
            "sowing_date": None,
            "missing_slots": []
        }

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def validate_location(location: str) -> None:
    """Raise 400 if the extracted location is clearly invalid."""
    if not location or len(location) < 2:
        raise HTTPException(
            status_code=400,
            detail="Could not extract a valid location from your query. "
                   "Try: 'weather in Mumbai' or 'rainfall in Delhi'.",
        )
    if re.fullmatch(r"[\W\d\s]+", location):
        raise HTTPException(
            status_code=400,
            detail=f"Extracted location '{location}' looks invalid. Please be more specific.",
        )


# ──────────────────────────────────────────────
# Simple in-process TTL cache (no Redis needed)
# ──────────────────────────────────────────────
from typing import Any, Tuple

_cache: Dict[str, Tuple[Any, float]] = {}
CACHE_TTL_SECONDS = 300   # 5 minutes


def cache_get(key: str) -> Optional[Any]:
    entry = _cache.get(key)
    if entry and (time_module.time() - entry[1]) < CACHE_TTL_SECONDS:
        return entry[0]
    return None


def cache_set(key: str, value: Any) -> None:
    _cache[key] = (value, time_module.time())


# ──────────────────────────────────────────────
# Coordinate resolver
# ──────────────────────────────────────────────
COORD_FIELD_PRIORITY = [
    ("village_latitude",       "village_longitude"),
    ("village_lat",            "village_lon"),
    ("mandal_latitude",        "mandal_longitude"),
    ("taluk_latitude",         "taluk_longitude"),
    ("subdistrict_latitude",   "subdistrict_longitude"),
    ("block_latitude",         "block_longitude"),
    ("district_latitude",      "district_longitude"),
    ("district_lat",           "district_lon"),
    ("state_latitude",         "state_longitude"),
    ("state_lat",              "state_lon"),
]

def resolve_best_coords(doc: dict):
    """
    Pick the most precise available lat/lon from a Solr doc.
    Tries village → mandal → taluk → district → state.
    Returns (lat, lon) or (None, None).
    """
    for lat_field, lon_field in COORD_FIELD_PRIORITY:
        lat = doc.get(lat_field)
        lon = doc.get(lon_field)
        if lat and lon:
            # Solr returns arrays — unwrap if needed
            lat = lat[0] if isinstance(lat, list) else lat
            lon = lon[0] if isinstance(lon, list) else lon
            if lat and lon:
                return lat, lon

    return None, None


# ──────────────────────────────────────────────
# Pydantic models
# ──────────────────────────────────────────────

class InfestationRequest(BaseModel):
    sowing_date: Optional[str] = "10-01-2026"
    crop_slug:   Optional[str] = "paddy"
    state_name:  Optional[str] = "Odisha"


# ──────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────

@app.get("/api/search")
async def search_location(q: str = Query(..., min_length=2)):
    """
    Accepts a natural-language query, extracts location + metric intent,
    and searches Solr for matching locations.
    """
    intent = await asyncio.to_thread(extract_intent, q)
    extracted_q = intent["location"]

    validate_location(extracted_q)

    cache_key = f"search:{extracted_q}"
    cached = cache_get(cache_key)
    if cached:
        result = cached.copy()
        result["intent"] = intent
        return result

    solr_q = (
        f'(village:"{extracted_q}" OR state:"{extracted_q}" OR district:"{extracted_q}")'
        f' OR (village:{extracted_q}~1 OR state:{extracted_q}~1 OR district:{extracted_q}~1)'
    )
    params = {"q": solr_q, "rows": 8, "wt": "json"}
    headers = {"Authorization": SOLR_AUTH}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(SOLR_SEARCH_URL, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

            if data.get("response", {}).get("docs"):
                first_doc = data["response"]["docs"][0]
                coord_fields = {k: v for k, v in first_doc.items() if 'lat' in k.lower() or 'lon' in k.lower() or 'long' in k.lower()}

                for doc in data["response"]["docs"]:
                    doc["_best_lat"], doc["_best_lon"] = resolve_best_coords(doc)

            if "response" in data:
                data["intent"] = intent

            cache_set(cache_key, data)
            return data

        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Location search timed out. Please try again.")
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=f"Search API error: {exc}")
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Internal server error: {exc}")


@app.get("/api/debug/location")
async def debug_location(q: str = Query(...)):
    """
    Debug endpoint — returns the raw full Solr doc for a location query
    so you can inspect every field and coordinate available.
    Usage: GET /api/debug/location?q=mumbai
    """
    solr_q = (
        f'(village:"{q}" OR state:"{q}" OR district:"{q}")'
        f' OR (village:{q}~1 OR state:{q}~1 OR district:{q}~1)'
    )
    params = {"q": solr_q, "rows": 3, "wt": "json"}
    headers = {"Authorization": SOLR_AUTH}

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(SOLR_SEARCH_URL, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        docs = data.get("response", {}).get("docs", [])

        # Annotate each doc with resolved coords
        for doc in docs:
            doc["_best_lat"], doc["_best_lon"] = resolve_best_coords(doc)
            doc["_all_coord_fields"] = {k: v for k, v in doc.items() if 'lat' in k.lower() or 'lon' in k.lower() or 'long' in k.lower()}

        return {"query": q, "numFound": data["response"]["numFound"], "docs": docs}


def filter_by_date(records: list, target_date, day_offset: int, date_field: str = "Date_time") -> list:
    """
    Filter forecast records to match the requested day.
    - day_offset == 0  → return today's record only
    - day_offset > 0   → return the specific future date's record
    - day_offset < 0   → past date requested; GFS only has today+future,
                         so return empty list with a warning
    """
    from datetime import date as date_type
    if isinstance(target_date, str):
        from datetime import date
        target_date = date.fromisoformat(target_date)

    if day_offset < 0:
        return []

    matched, available = [], []
    for record in records:
        raw = record.get(date_field, "")
        try:
            record_date = datetime.fromisoformat(raw).date()
            available.append(str(record_date))
            if record_date == target_date:
                matched.append(record)
        except (ValueError, TypeError):
            pass

    return matched


def filter_from_today(records: list, date_field: str = "Date_time") -> list:
    """Drop records before today — used when no specific date is requested."""
    from datetime import timedelta
    ist_offset  = timedelta(hours=5, minutes=30)
    today_ist   = (datetime.now(timezone.utc) + ist_offset).date()

    filtered, skipped = [], []
    for record in records:
        raw = record.get(date_field, "")
        try:
            record_date = datetime.fromisoformat(raw).date()
            if record_date >= today_ist:
                filtered.append(record)
            else:
                skipped.append(str(record_date))
        except (ValueError, TypeError):
            filtered.append(record)

    return filtered


@app.get("/api/weather/daily")
async def get_daily_weather(
    lat:         float,
    lon:         float,
    days:        Optional[int] = Query(default=None, ge=1, le=16),
    target_date: Optional[str] = Query(default=None),
    day_offset:  Optional[str] = Query(default=None),  # ← str, not int
    query_type:  Optional[str] = Query(default="single"),
    range_days:  Optional[int] = Query(default=None, ge=1, le=16),
    condition:   Optional[str] = Query(default=None),
):
    # ── Sanitize inputs ──────────────────────────────────────────────
    # 1. Parse day_offset safely — frontend may send "null" as a string
    _day_offset: Optional[int] = None
    if day_offset is not None and day_offset.strip() not in ("null", "none", "", "undefined"):
        try:
            _day_offset = int(day_offset)
        except ValueError:
            _day_offset = None
    day_offset = _day_offset

    # 2. Range/conditional queries never need day_offset or target_date
    RANGE_TYPES = {"range", "range_few", "range_week", "conditional_rain", "conditional_condition"}
    if query_type in RANGE_TYPES:
        day_offset  = None
        target_date = None

    # 3. condition param is only meaningful for conditional_condition
    if query_type != "conditional_condition":
        condition = None

    # ── rest of your existing code unchanged from here ───────────────
    cache_key = f"daily:{lat}:{lon}:{query_type}:{target_date}:{day_offset}:{days}:{range_days}:{condition}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    from datetime import timedelta, date as date_cls
    ist_offset    = timedelta(hours=5, minutes=30)
    today_ist     = (datetime.now(timezone.utc) + ist_offset).date()
    resolved_date = None
    resolved_offset = None

    if target_date:
        try:
            resolved_date = date_cls.fromisoformat(target_date)
            resolved_offset = (resolved_date - today_ist).days
            day_offset = resolved_offset   
            print(day_offset)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid date '{target_date}'. Use YYYY-MM-DD.")
    elif day_offset is not None:
        resolved_offset = day_offset
        resolved_date   = today_ist + timedelta(days=day_offset)

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.get(GFS_INTERPOLATE_URL, params={"lat": lat, "lon": lon})
            response.raise_for_status()
            data = response.json()

            def apply_filter(records: list):
                """Returns (filtered_records, message_or_None)"""

                # Range: next N days
                if query_type in ("range", "range_few", "range_week"):
                    n      = range_days or days or 7
                    result = filter_from_today(records)[:n]
                    return result, None

                # Conditional: when will it rain
                if query_type == "conditional_rain":
                    future = filter_from_today(records)

                    if range_days == 1 and resolved_date is not None:
                        future = [r for r in future
                                  if datetime.fromisoformat(r.get("Date_time","")).date() == resolved_date]
                    elif range_days:
                        future = future[:range_days]

                    rainy_days = [r for r in future if (r.get("Rainfall") or 0) > 0]
                    if not rainy_days:
                        if range_days == 1 and resolved_date:
                            return [], f"☀️ No rainfall expected in the forecast for {resolved_date.strftime('%A, %d %b')}."
                        window_desc = f"next {range_days} days" if range_days else "forecast period"
                        return [], f"☀️ No rainfall expected in the {window_desc}."
                    return rainy_days, None

                # Conditional: hot/cold/windy/humid
                if query_type == "conditional_condition":
                    future = filter_from_today(records)
                    COND = {
                        "hot":   lambda r: r.get("Tmax", 0) > 38,
                        "warm":  lambda r: r.get("Tmax", 0) > 30,
                        "cold":  lambda r: r.get("Tmin", 99) < 15,
                        "windy": lambda r: r.get("Wind_Speed", 0) > 20,
                        "humid": lambda r: r.get("RH", 0) > 80,
                    }
                    fn      = COND.get(condition or "", lambda r: True)
                    matched = [r for r in future if fn(r)]
                    if not matched:
                        return [], f"No '{condition}' conditions expected in the forecast period."
                    return matched, None

                # Single day
                if resolved_date is not None:
                    if resolved_offset < 0:
                        return [], f"No historical data for {resolved_date}. GFS only provides forecasts from today onwards."
                    result = filter_by_date(records, resolved_date, resolved_offset)
                    return result, None

                # Default: all from today
                result = filter_from_today(records)
                if days:
                    result = result[:days]
                return result, None

            extra_msg = None
            if isinstance(data, dict):
                fkey = next((k for k in data if isinstance(data[k], list)), None)
                if fkey:
                    data[fkey], extra_msg = apply_filter(data[fkey])
            elif isinstance(data, list):
                filtered, extra_msg = apply_filter(data)
                data = {"Forecast data": filtered}

            if extra_msg:
                data["_message"] = extra_msg

            record_count = len(data.get("Forecast data", data.get(list(data.keys())[0], [])) if isinstance(data, dict) else data)

            cache_set(cache_key, data)
            return data

        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Daily weather fetch timed out.")
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=f"GFS API error: {exc}")
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Internal server error: {exc}")


@app.get("/api/weather/hourly")
async def get_hourly_weather(
    lat: float,
    lon: float,
    hours: Optional[int] = Query(default=24, ge=1, le=240, description="Number of hours to return (default 24)"),
):
    """
    Fetch hourly weather forecast.
    - Strips records before the current hour (IST).
    - Returns only the first `hours` entries (default 24).
    """
    cache_key = f"hourly:{lat}:{lon}:{hours}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    params = {"lat": lat, "lon": lon}

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.get(GFS_HOURLY_URL, params=params)
            response.raise_for_status()
            data = response.json()

            from datetime import timedelta
            ist_offset = timedelta(hours=5, minutes=30)
            now_ist = datetime.now(timezone.utc) + ist_offset

            def filter_hourly(records: list) -> list:
                filtered, skipped = [], []
                for r in records:
                    raw = r.get("Date_time", r.get("datetime", r.get("time", "")))
                    try:
                        dt = datetime.fromisoformat(raw)
                        if dt >= now_ist.replace(tzinfo=None):
                            filtered.append(r)
                        else:
                            skipped.append(raw)
                    except (ValueError, TypeError):
                        filtered.append(r)
                return filtered

            if isinstance(data, dict):
                forecast_key = next((k for k in data if isinstance(data[k], list)), None)
                if forecast_key:
                    data[forecast_key] = filter_hourly(data[forecast_key])[:hours]
            elif isinstance(data, list):
                data = filter_hourly(data)[:hours]

            cache_set(cache_key, data)
            return data

        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Hourly weather fetch timed out.")
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=f"Hourly API error: {exc}")
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Internal server error: {exc}")


# ──────────────────────────────────────────────
# Pest / Infestation endpoint
# ──────────────────────────────────────────────

@app.post("/api/pest/infestation")
async def get_pest_infestation(
    body: InfestationRequest,
    lat: float = Query(...),
    lon: float = Query(...),
    is_next_week: bool = Query(False),
):
    # 1. Validation check
    missing_fields = [f for f in ["crop_slug", "sowing_date"] if not getattr(body, f)]
    if missing_fields:
        return {"status": "need_info", "missing_slots": missing_fields}

    # ── Normalise sowing_date to DD-MM-YYYY ──────────────────────────
    # LLM returns ISO (YYYY-MM-DD), but GFS infestation API needs DD-MM-YYYY
    sowing_date = body.sowing_date
    if sowing_date and re.match(r"^\d{4}-\d{2}-\d{2}$", sowing_date):
        # YYYY-MM-DD → DD-MM-YYYY
        y, m, d = sowing_date.split("-")
        sowing_date = f"{d}-{m}-{y}"

    # 2. Cache check
    cache_key = f"pest:{lat}:{lon}:{is_next_week}:{sowing_date}:{body.crop_slug}:{body.state_name}"
    if (cached := cache_get(cache_key)):
        return cached

    # 3. Data Fetch — use normalised sowing_date
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.post(
            GFS_INFESTATION_URL,
            params={"lat": lat, "lon": lon, "is_next_week": str(is_next_week).lower()},
            json={
                "sowing_date": sowing_date,          # ← normalised
                "crop_slug":   body.crop_slug,
                "state_name":  body.state_name
            }
        )
        response.raise_for_status()
        data = response.json()

    # 4. Processing Pipeline
    # Only iterate over the data list ONCE
    active_threats = []
    clear_threats = []
    week_key = "next_week" if is_next_week else "current_week"

    if isinstance(data.get("data"), list):
        for item in data["data"]:
            pct = (item.get("chances_percentage") or {}).get(week_key) or 0
            name = item.get("infestation_name", "Unknown")
            
            if pct > 0:
                active_threats.append({
                    "name": name,
                    "probability": pct,
                    "risk_level": _risk_label(pct),
                    **item
                })
            else:
                clear_threats.append(name)

    # 5. Finalize and Cache
    data.update({
        "active_threats": sorted(active_threats, key=lambda x: x["probability"], reverse=True),
        "clear_threats": clear_threats,
        "summary": {"active_count": len(active_threats), "week": week_key}
    })
    
    cache_set(cache_key, data)
    return data


def _risk_label(probability: float) -> str:
    """Convert a probability percentage to a human-readable risk level."""
    if probability >= 75:
        return "High"
    if probability >= 40:
        return "Medium"
    if probability > 0:
        return "Low"
    return "None"


# ──────────────────────────────────────────────
# Health check
# ──────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8010)