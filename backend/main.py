from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
import re
import time as time_module
from datetime import date, datetime, timezone
from typing import Optional, Dict

app = FastAPI(title="WeatherSnap ChatBot Backend")

# ──────────────────────────────────────────────
# Configuration  (use environment variables;
# fall back to defaults only for local dev)
# ──────────────────────────────────────────────
SOLR_SEARCH_URL  = os.getenv("SOLR_SEARCH_URL",  "https://solr.apps.niruthi.com/solr/location_data/select")
SOLR_AUTH        = os.getenv("SOLR_AUTH_TOKEN",   "Basic YXBpOk5pcnV0aGlAMjRVc2Vy")   # move to .env in production!
GFS_INTERPOLATE_URL = os.getenv("GFS_INTERPOLATE_URL", "https://gfsapi.niruthiapptesting.com/interpolate")
GFS_HOURLY_URL   = os.getenv("GFS_HOURLY_URL",    "https://gfsapi.niruthiapptesting.com/hrlydata")

# Production: restrict to your frontend origin
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# ──────────────────────────────────────────────
# Request/Response logging middleware
# ──────────────────────────────────────────────
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time_module.time()
    try:
        response = await call_next(request)
        duration = round((time_module.time() - start) * 1000, 1)
        return response
    except Exception as exc:
        duration = round((time_module.time() - start) * 1000, 1)
        raise

# ──────────────────────────────────────────────
# CORS
# ──────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────
# NLU helpers
# ──────────────────────────────────────────────

# Words to strip when isolating the *location* (does NOT include metric words
# so that metric extraction happens independently and cleanly)
LOCATION_STOP_WORDS = {
    "what", "is", "today", "the", "weather", "forecast", "in", "at", "for",
    "of", "how", "show", "me", "look", "like", "will", "be", "are", "tell",
    "give", "get", "please", "current", "latest", "update", "about", "a", "an",
}

# Time qualifiers that should not bleed into the location string
TIME_WORDS = {
    "today", "tomorrow", "tonight", "yesterday", "now", "currently", "next", "this",
    "week", "month", "year", "hour", "hours", "day", "days",
    "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
    "morning", "afternoon", "evening", "night", "midnight", "noon",
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",
}

TIME_INTENT_MAP = [
    ("day after tomorrow",  2),
    ("day before yesterday",-2),
    ("next week",           7),
    ("yesterday",          -1),
    ("tomorrow",            1),
    ("tonight",             0),
    ("today",               0),
    ("now",                 0),
    ("currently",           0),
    ("this week",           0),
    ("next monday",         None), 
    ("next tuesday",        None),
    ("next wednesday",      None),
    ("next thursday",       None),
    ("next friday",         None),
    ("next saturday",       None),
    ("next sunday",         None),
    ("monday",              None),
    ("tuesday",             None),
    ("wednesday",           None),
    ("thursday",            None),
    ("friday",              None),
    ("saturday",            None),
    ("sunday",              None),
]

WEEKDAY_NAMES = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]

MULTI_DAY_PATTERNS = [
    (re.compile(r"(?:next|coming|in(?:\s+the)?)\s+(\d+)\s+days?"), "range"),
    (re.compile(r"(?:next|coming)\s+(?:few|couple\s+of|some)\s+days?"),  "range_few"),
    (re.compile(r"(?:this|next|coming)\s+week"),                          "range_week"),
    (re.compile(r"\bwhen\s+(?:will|does|is|would)\s+it\s+rain\b"),       "conditional_rain"),
    (re.compile(r"\bwhen\s+(?:will|is|does)\s+(?:there\s+be\s+)?rain\b"),"conditional_rain"),
    (re.compile(r"\bwill\s+it\s+rain\b"),                                 "conditional_rain"),
    (re.compile(r"\bwhen\s+will\s+it\s+be\s+(hot|cold|windy|humid|warm)\b"), "conditional_condition"),
]

def resolve_day_offset(phrase: str) -> int:
    """Convert a weekday name to a day offset from today."""
    from datetime import timedelta
    target_name = phrase.replace("next ", "").strip()
    target_idx  = WEEKDAY_NAMES.index(target_name)
    today_idx   = datetime.now(timezone.utc).weekday()
    delta = (target_idx - today_idx) % 7
    if delta == 0:
        delta = 7   
    return delta

METRIC_MAP = {
    "maximum temperature": "Tmax",
    "max temperature":     "Tmax",
    "max temp":            "Tmax",
    "minimum temperature": "Tmin",
    "min temperature":     "Tmin",
    "min temp":            "Tmin",
    "average temperature": "Tavg",
    "avg temperature":     "Tavg",
    "avg temp":            "Tavg",
    "wind speed":          "Wind_Speed",
    "wind":                "Wind_Speed",
    "rainfall":            "Rainfall",
    "rain":                "Rainfall",
    "humidity":            "RH",
    "humid":               "RH",
    "temperature":         "Tavg",  
    "temp":                "Tavg",
}

METRIC_WORDS = {
    "max", "min", "maximum", "minimum", "average", "avg",
    "temperature", "temp", "rain", "rainfall", "humidity", "humid",
    "wind", "speed",
}


def extract_intent(query: str) -> Dict:
    """
    Parse a free-text weather query and return:
      - location    : cleaned place name
      - metric      : one of the METRIC_MAP values, or "ALL"
      - query_type  : "single" | "range" | "conditional_rain" | "conditional_condition"
      - day_offset  : int (single-day queries only)
      - target_date : ISO string (single-day queries only)
      - range_days  : int (range queries — how many days to return)
      - condition   : str (conditional queries — what to look for, e.g. "rain", "hot")
    """
    q = query.lower().strip()

    from datetime import timedelta
    ist_offset = timedelta(hours=5, minutes=30)
    today_ist  = (datetime.now(timezone.utc) + ist_offset).date()

    # ── 1. Extract metric ────────────────────────────────────────────────────
    metric = None
    for phrase, value in METRIC_MAP.items():
        if phrase in q:
            metric = value
            break


    # ── 2. Check for multi-day / conditional patterns FIRST ─────────────────
    query_type = "single"
    range_days = None
    condition  = None

    for pattern, ptype in MULTI_DAY_PATTERNS:
        m = pattern.search(q)
        if m:
            query_type = ptype

            if ptype == "range":
                range_days = int(m.group(1))

            elif ptype == "range_few":
                range_days = 5   # "few days" → 5

            elif ptype == "range_week":
                range_days = 7

            elif ptype == "conditional_rain":
                query_type = "conditional_rain"
                condition  = "rain"
                metric     = "Rainfall"   # override metric to Rainfall

            elif ptype == "conditional_condition":
                query_type = "conditional_condition"
                condition  = m.group(1)   # "hot", "cold", "windy", "humid", "warm"

            break


    day_offset  = 0
    target_date = today_ist

    for phrase, offset in TIME_INTENT_MAP:
        if phrase in q:
            if offset is None:
                day_offset = resolve_day_offset(phrase)
            else:
                day_offset = offset
            break

    target_date = today_ist + timedelta(days=day_offset)

    SPECIFIC_DAY_WORDS = {"tomorrow", "tonight", "today", "now", "yesterday",
                          "monday","tuesday","wednesday","thursday","friday","saturday","sunday",
                          "day after tomorrow"}
    has_specific_day = any(word in q for word in SPECIFIC_DAY_WORDS)

    if query_type == "conditional_rain" and has_specific_day and not range_days:
        range_days = 1

    # ── 4. Extract location ──────────────────────────────────────────────────
    location = q

    preposition_patterns = [
        r"\bin\s+([a-z][a-z\s]+?)(?:\s+(?:today|tomorrow|tonight|yesterday|next|this|now|currently|in\s+next|day\s+after|day\s+before|when)|\?|$)",
        r"\bat\s+([a-z][a-z\s]+?)(?:\s+(?:today|tomorrow|tonight|yesterday|next|this|now|currently)|\?|$)",
        r"\bfor\s+([a-z][a-z\s]+?)(?:\s+(?:today|tomorrow|tonight|yesterday|next|this|now|currently)|\?|$)",
        r"\bof\s+([a-z][a-z\s]+?)(?:\s+(?:today|tomorrow|tonight|yesterday|next|this|now|currently)|\?|$)",
    ]
    matched = False
    for pattern in preposition_patterns:
        m = re.search(pattern, q)
        if m:
            location = m.group(1).strip()
            matched = True
            break

    if not matched:
        combined_stop = LOCATION_STOP_WORDS | METRIC_WORDS | TIME_WORDS
        words = q.split()
        location_words = [w for w in words if w not in combined_stop]
        location = " ".join(location_words).strip()

    location_tokens = [w for w in location.split() if w not in TIME_WORDS]
    location = re.sub(r"[^\w\s]", "", " ".join(location_tokens)).strip()

    final_location = location or query.strip()
    final_metric   = metric or "ALL"

    return {
        "location":    final_location,
        "metric":      final_metric,
        "query_type":  query_type,
        "day_offset":  day_offset,
        "target_date": str(target_date),
        "range_days":  range_days,
        "condition":   condition,
    }


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
# Routes
# ──────────────────────────────────────────────

@app.get("/api/search")
async def search_location(q: str = Query(..., min_length=2)):
    """
    Accepts a natural-language query, extracts location + metric intent,
    and searches Solr for matching locations.
    """
    intent = extract_intent(q)
    extracted_q = intent["location"]

    validate_location(extracted_q)

    cache_key = f"search:{extracted_q}"
    cached = cache_get(cache_key)
    if cached:
        cached["intent"] = intent
        return cached

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

            num_results = data.get("response", {}).get("numFound", "?")

            # ── Log ALL fields of first doc so we can see every coord field available ──
            if data.get("response", {}).get("docs"):
                first_doc = data["response"]["docs"][0]
                coord_fields = {k: v for k, v in first_doc.items() if 'lat' in k.lower() or 'lon' in k.lower() or 'long' in k.lower()}

                # ── Inject best_coords into each doc so frontend always uses right lat/lon ──
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
    day_offset:  Optional[int] = Query(default=None),
    query_type:  Optional[str] = Query(default="single"),
    range_days:  Optional[int] = Query(default=None, ge=1, le=16),
    condition:   Optional[str] = Query(default=None),
):
    """
    query_type=single            -> one specific day
    query_type=range             -> range_days days from today
    query_type=conditional_rain  -> days where Rainfall > 0
    query_type=conditional_condition -> days matching hot/cold/windy/humid
    """
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
            resolved_date   = date_cls.fromisoformat(target_date)
            resolved_offset = (resolved_date - today_ist).days
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
# Health check
# ──────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8010)