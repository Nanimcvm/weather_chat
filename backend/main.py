from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
import re
import logging
import time as time_module
from datetime import date, datetime, timezone
from typing import Optional, Dict

# ──────────────────────────────────────────────
# Logging setup — outputs to console with timestamps
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("weathersnap")

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
    logger.info(f"➡️  REQUEST  {request.method} {request.url}")
    logger.debug(f"   Headers : {dict(request.headers)}")
    try:
        response = await call_next(request)
        duration = round((time_module.time() - start) * 1000, 1)
        logger.info(f"⬅️  RESPONSE {response.status_code} — {duration}ms")
        return response
    except Exception as exc:
        duration = round((time_module.time() - start) * 1000, 1)
        logger.error(f"💥 UNHANDLED EXCEPTION after {duration}ms: {exc}", exc_info=True)
        raise

# ──────────────────────────────────────────────
# CORS
# ──────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,   # set ALLOWED_ORIGINS=https://wsnap.niruthiapptesting.com in prod
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

# Maps time phrases → day offset relative to today (0=today, 1=tomorrow, -1=yesterday, etc.)
# Ordered longest-first so "day after tomorrow" matches before "tomorrow"
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
    ("next monday",         None),   # weekday — resolved dynamically below
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

def resolve_day_offset(phrase: str) -> int:
    """Convert a weekday name to a day offset from today."""
    from datetime import timedelta
    target_name = phrase.replace("next ", "").strip()
    target_idx  = WEEKDAY_NAMES.index(target_name)
    today_idx   = datetime.now(timezone.utc).weekday()
    delta = (target_idx - today_idx) % 7
    if delta == 0:
        delta = 7   # "next monday" when today is monday → 7 days ahead
    return delta

# Ordered from most-specific to least-specific so longer phrases match first
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
    "temperature":         "Tavg",   # generic "temperature" → average
    "temp":                "Tavg",
}

# All words that appear in metric phrases (to help strip them from location)
METRIC_WORDS = {
    "max", "min", "maximum", "minimum", "average", "avg",
    "temperature", "temp", "rain", "rainfall", "humidity",
    "wind", "speed",
}


def extract_intent(query: str) -> Dict:
    """
    Parse a free-text weather query and return:
      - location   : cleaned place name
      - metric     : one of the METRIC_MAP values, or "ALL" if unspecified
      - day_offset : int — 0=today, 1=tomorrow, -1=yesterday, etc. (default 0)
      - target_date: ISO date string for the requested day (e.g. '2026-03-13')
    """
    logger.debug(f"[NLU] Raw query: '{query}'")
    q = query.lower().strip()

    # ── 1. Extract metric (longest-match first) ──────────────────────────────
    metric = None
    for phrase, value in METRIC_MAP.items():
        if phrase in q:
            metric = value
            logger.debug(f"[NLU] Metric matched: '{phrase}' → '{value}'")
            break
    if metric is None:
        logger.debug("[NLU] No metric keyword found — will default to 'ALL'")

    # ── 2. Extract time intent ────────────────────────────────────────────────
    from datetime import timedelta
    ist_offset = timedelta(hours=5, minutes=30)
    today_ist  = (datetime.now(timezone.utc) + ist_offset).date()

    day_offset  = 0
    time_phrase = "today"
    for phrase, offset in TIME_INTENT_MAP:
        if phrase in q:
            if offset is None:
                day_offset = resolve_day_offset(phrase)
            else:
                day_offset = offset
            time_phrase = phrase
            logger.debug(f"[NLU] Time intent matched: '{phrase}' → day_offset={day_offset}")
            break
    else:
        logger.debug("[NLU] No time phrase found — defaulting to today (day_offset=0)")

    target_date = today_ist + timedelta(days=day_offset)
    logger.debug(f"[NLU] Target date: {target_date} (offset={day_offset:+d} from today {today_ist})")

    # ── 3. Extract location ───────────────────────────────────────────────────
    location = q

    preposition_patterns = [
        r"\bin\s+([a-z][a-z\s]+?)(?:\s+(?:today|tomorrow|tonight|yesterday|next|this|now|currently|day\s+after|day\s+before)|\?|$)",
        r"\bat\s+([a-z][a-z\s]+?)(?:\s+(?:today|tomorrow|tonight|yesterday|next|this|now|currently)|\?|$)",
        r"\bfor\s+([a-z][a-z\s]+?)(?:\s+(?:today|tomorrow|tonight|yesterday|next|this|now|currently)|\?|$)",
        r"\bof\s+([a-z][a-z\s]+?)(?:\s+(?:today|tomorrow|tonight|yesterday|next|this|now|currently)|\?|$)",
    ]
    matched = False
    for pattern in preposition_patterns:
        m = re.search(pattern, q)
        if m:
            location = m.group(1).strip()
            logger.debug(f"[NLU] Location via preposition regex: '{location}'")
            matched = True
            break

    if not matched:
        logger.debug("[NLU] No preposition pattern matched — falling back to stop-word stripping")
        combined_stop = LOCATION_STOP_WORDS | METRIC_WORDS | TIME_WORDS
        words = q.split()
        location_words = [w for w in words if w not in combined_stop]
        logger.debug(f"[NLU] Words after stop-word removal: {location_words}")
        location = " ".join(location_words).strip()

    before_time_strip = location
    location_tokens = [w for w in location.split() if w not in TIME_WORDS]
    location = " ".join(location_tokens).strip()
    if before_time_strip != location:
        logger.debug(f"[NLU] Time words stripped: '{before_time_strip}' → '{location}'")

    before_punct = location
    location = re.sub(r"[^\w\s]", "", location).strip()
    if before_punct != location:
        logger.debug(f"[NLU] Punctuation stripped: '{before_punct}' → '{location}'")

    final_location = location or query.strip()
    final_metric   = metric or "ALL"

    if not location:
        logger.warning(f"[NLU] Location extraction empty — using raw query: '{query.strip()}'")

    logger.info(f"[NLU] Intent → location='{final_location}', metric='{final_metric}', day_offset={day_offset:+d}, target_date={target_date}")
    return {
        "location":    final_location,
        "metric":      final_metric,
        "day_offset":  day_offset,
        "target_date": str(target_date),
    }


def validate_location(location: str) -> None:
    """Raise 400 if the extracted location is clearly invalid."""
    logger.debug(f"[VALIDATE] Checking location: '{location}'")
    if not location or len(location) < 2:
        logger.warning(f"[VALIDATE] ❌ Location too short or empty: '{location}'")
        raise HTTPException(
            status_code=400,
            detail="Could not extract a valid location from your query. "
                   "Try: 'weather in Mumbai' or 'rainfall in Delhi'.",
        )
    if re.fullmatch(r"[\W\d\s]+", location):
        logger.warning(f"[VALIDATE] ❌ Location looks invalid (no letters): '{location}'")
        raise HTTPException(
            status_code=400,
            detail=f"Extracted location '{location}' looks invalid. Please be more specific.",
        )
    logger.debug(f"[VALIDATE] ✅ Location '{location}' passed validation")


# ──────────────────────────────────────────────
# Simple in-process TTL cache (no Redis needed)
# ──────────────────────────────────────────────
from typing import Any, Tuple

_cache: Dict[str, Tuple[Any, float]] = {}
CACHE_TTL_SECONDS = 300   # 5 minutes


def cache_get(key: str) -> Optional[Any]:
    entry = _cache.get(key)
    if entry and (time_module.time() - entry[1]) < CACHE_TTL_SECONDS:
        logger.debug(f"[CACHE] ✅ HIT  — key='{key}'")
        return entry[0]
    if entry:
        logger.debug(f"[CACHE] ⏰ EXPIRED — key='{key}'")
    else:
        logger.debug(f"[CACHE] ❌ MISS  — key='{key}'")
    return None


def cache_set(key: str, value: Any) -> None:
    _cache[key] = (value, time_module.time())
    logger.debug(f"[CACHE] 💾 SET   — key='{key}'")


# ──────────────────────────────────────────────
# Coordinate resolver
# ──────────────────────────────────────────────

# All known lat/lon field name patterns in Solr docs, ordered most-precise first.
# The website uses the most precise available coordinate — we must do the same.
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
                logger.debug(f"[COORDS] Resolved via '{lat_field}': lat={lat}, lon={lon}")
                return lat, lon

    logger.warning(f"[COORDS] ⚠️ No coordinate fields found in doc: {list(doc.keys())}")
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
    logger.info(f"[SEARCH] Received query: '{q}'")
    intent = extract_intent(q)
    extracted_q = intent["location"]

    validate_location(extracted_q)

    cache_key = f"search:{extracted_q}"
    cached = cache_get(cache_key)
    if cached:
        cached["intent"] = intent
        logger.info(f"[SEARCH] Returning cached result for location='{extracted_q}'")
        return cached

    solr_q = (
        f'(village:"{extracted_q}" OR state:"{extracted_q}" OR district:"{extracted_q}")'
        f' OR (village:{extracted_q}~1 OR state:{extracted_q}~1 OR district:{extracted_q}~1)'
    )
    params = {"q": solr_q, "rows": 8, "wt": "json"}
    headers = {"Authorization": SOLR_AUTH}

    logger.debug(f"[SEARCH] Solr URL  : {SOLR_SEARCH_URL}")
    logger.debug(f"[SEARCH] Solr query: {solr_q}")

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(SOLR_SEARCH_URL, params=params, headers=headers)
            logger.debug(f"[SEARCH] Solr HTTP status: {response.status_code}")
            logger.debug(f"[SEARCH] Solr raw response: {response.text[:500]}")
            response.raise_for_status()
            data = response.json()

            num_results = data.get("response", {}).get("numFound", "?")
            logger.info(f"[SEARCH] Solr returned {num_results} result(s) for '{extracted_q}'")

            # ── Log ALL fields of first doc so we can see every coord field available ──
            if data.get("response", {}).get("docs"):
                first_doc = data["response"]["docs"][0]
                logger.debug(f"[SEARCH] First doc ALL fields: {first_doc}")
                coord_fields = {k: v for k, v in first_doc.items() if 'lat' in k.lower() or 'lon' in k.lower() or 'long' in k.lower()}
                logger.info(f"[SEARCH] First doc coordinate fields: {coord_fields}")

                # ── Inject best_coords into each doc so frontend always uses right lat/lon ──
                for doc in data["response"]["docs"]:
                    doc["_best_lat"], doc["_best_lon"] = resolve_best_coords(doc)
                    logger.debug(f"[SEARCH] Doc '{doc.get('village', doc.get('district', ['?']))[0]}' → best_lat={doc['_best_lat']}, best_lon={doc['_best_lon']}")

            if "response" in data:
                data["intent"] = intent

            cache_set(cache_key, data)
            return data

        except httpx.TimeoutException:
            logger.error(f"[SEARCH] ⏱ Solr request timed out for query='{extracted_q}'")
            raise HTTPException(status_code=504, detail="Location search timed out. Please try again.")
        except httpx.HTTPStatusError as exc:
            logger.error(f"[SEARCH] ❌ Solr HTTP error {exc.response.status_code}: {exc.response.text[:300]}")
            raise HTTPException(status_code=exc.response.status_code, detail=f"Search API error: {exc}")
        except Exception as exc:
            logger.error(f"[SEARCH] 💥 Unexpected error: {exc}", exc_info=True)
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

    logger.debug(f"[DATE-FILTER] Filtering for target_date={target_date}, day_offset={day_offset:+d}")

    if day_offset < 0:
        logger.warning(f"[DATE-FILTER] ⚠️  User asked for past date ({target_date}) — GFS has no historical data, returning empty")
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
            logger.warning(f"[DATE-FILTER] Could not parse date '{raw}' — skipping record")

    logger.debug(f"[DATE-FILTER] Available dates in GFS response: {sorted(set(available))}")
    logger.info(f"[DATE-FILTER] Matched {len(matched)} record(s) for target date {target_date}")

    if not matched:
        logger.warning(f"[DATE-FILTER] ⚠️  No records found for {target_date}. Available: {sorted(set(available))}")

    return matched


def filter_from_today(records: list, date_field: str = "Date_time") -> list:
    """Drop records before today — used when no specific date is requested."""
    from datetime import timedelta
    ist_offset  = timedelta(hours=5, minutes=30)
    today_ist   = (datetime.now(timezone.utc) + ist_offset).date()
    logger.debug(f"[DATE-FILTER] Filtering from today ({today_ist}) onwards")

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
            logger.warning(f"[DATE-FILTER] Could not parse date '{raw}' — keeping record")
            filtered.append(record)

    if skipped:
        logger.info(f"[DATE-FILTER] Dropped {len(skipped)} stale record(s): {skipped}")
    logger.info(f"[DATE-FILTER] Kept {len(filtered)} record(s) from today onwards")
    return filtered


@app.get("/api/weather/daily")
async def get_daily_weather(
    lat: float,
    lon: float,
    days: Optional[int]  = Query(default=None, ge=1, le=16,  description="Return N days from today (ignored if target_date is set)"),
    target_date: Optional[str] = Query(default=None, description="ISO date YYYY-MM-DD — return only this specific day"),
    day_offset: Optional[int]  = Query(default=None, description="Day offset from today: 0=today, 1=tomorrow, -1=yesterday"),
):
    """
    Fetch daily weather forecast.
    Priority: target_date > day_offset > days > all-from-today
    """
    logger.info(f"[DAILY] Request — lat={lat}, lon={lon}, days={days}, target_date={target_date}, day_offset={day_offset}")

    cache_key = f"daily:{lat}:{lon}:{target_date}:{day_offset}:{days}"
    cached = cache_get(cache_key)
    if cached:
        logger.info(f"[DAILY] Returning cached result")
        return cached

    # Resolve target_date from day_offset if not explicitly provided
    from datetime import timedelta, date as date_type
    ist_offset = timedelta(hours=5, minutes=30)
    today_ist  = (datetime.now(timezone.utc) + ist_offset).date()

    resolved_date = None
    resolved_offset = None

    if target_date:
        try:
            from datetime import date
            resolved_date   = date.fromisoformat(target_date)
            resolved_offset = (resolved_date - today_ist).days
            logger.debug(f"[DAILY] Using explicit target_date={resolved_date} (offset={resolved_offset:+d})")
        except ValueError:
            logger.error(f"[DAILY] Invalid target_date format: '{target_date}'")
            raise HTTPException(status_code=400, detail=f"Invalid date format '{target_date}'. Use YYYY-MM-DD.")
    elif day_offset is not None:
        resolved_offset = day_offset
        resolved_date   = today_ist + timedelta(days=day_offset)
        logger.debug(f"[DAILY] Resolved day_offset={day_offset} → date={resolved_date}")

    params = {"lat": lat, "lon": lon}
    logger.debug(f"[DAILY] Calling GFS: {GFS_INTERPOLATE_URL} params={params}")

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.get(GFS_INTERPOLATE_URL, params=params)
            logger.debug(f"[DAILY] GFS status={response.status_code}")
            logger.debug(f"[DAILY] GFS response preview: {response.text[:500]}")
            response.raise_for_status()
            data = response.json()

            logger.info(f"[DAILY] GFS keys={list(data.keys()) if isinstance(data, dict) else type(data).__name__}")

            def apply_filter(records: list) -> list:
                if resolved_date is not None:
                    # Specific day requested
                    result = filter_by_date(records, resolved_date, resolved_offset)
                    if not result and resolved_offset < 0:
                        # Past date — return helpful message instead of empty list
                        logger.warning("[DAILY] Past date requested — GFS has no historical data")
                    return result
                else:
                    # No specific day — return from today onwards, optionally sliced
                    result = filter_from_today(records)
                    if days:
                        result = result[:days]
                        logger.debug(f"[DAILY] Sliced to {days} days")
                    return result

            if isinstance(data, dict):
                forecast_key = next((k for k in data if isinstance(data[k], list)), None)
                if forecast_key:
                    logger.debug(f"[DAILY] Forecast key='{forecast_key}', {len(data[forecast_key])} raw records")
                    data[forecast_key] = apply_filter(data[forecast_key])
                else:
                    logger.warning("[DAILY] No list found in GFS dict — returning as-is")
            elif isinstance(data, list):
                data = apply_filter(data)

            # Add a user-friendly message when past date is requested
            if resolved_offset is not None and resolved_offset < 0:
                if isinstance(data, dict):
                    data["_message"] = f"No historical data available for {resolved_date}. GFS only provides forecasts from today onwards."
                    logger.info("[DAILY] Injected past-date warning into response")

            cache_set(cache_key, data)
            return data

        except httpx.TimeoutException:
            logger.error(f"[DAILY] ⏱ Timeout for lat={lat}, lon={lon}")
            raise HTTPException(status_code=504, detail="Daily weather fetch timed out.")
        except httpx.HTTPStatusError as exc:
            logger.error(f"[DAILY] ❌ HTTP {exc.response.status_code}: {exc.response.text[:300]}")
            raise HTTPException(status_code=exc.response.status_code, detail=f"Interpolate API error: {exc}")
        except Exception as exc:
            logger.error(f"[DAILY] 💥 {exc}", exc_info=True)
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
    logger.info(f"[HOURLY] Request — lat={lat}, lon={lon}, hours={hours}")

    cache_key = f"hourly:{lat}:{lon}:{hours}"
    cached = cache_get(cache_key)
    if cached:
        logger.info(f"[HOURLY] Returning cached result for lat={lat}, lon={lon}")
        return cached

    params = {"lat": lat, "lon": lon}
    logger.debug(f"[HOURLY] Calling GFS hourly URL: {GFS_HOURLY_URL} with params={params}")

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.get(GFS_HOURLY_URL, params=params)
            logger.debug(f"[HOURLY] GFS HTTP status: {response.status_code}")
            logger.debug(f"[HOURLY] GFS raw response (first 500 chars): {response.text[:500]}")
            response.raise_for_status()
            data = response.json()

            logger.info(f"[HOURLY] GFS returned data type={type(data).__name__}, keys={list(data.keys()) if isinstance(data, dict) else 'N/A'}")

            from datetime import timedelta
            ist_offset = timedelta(hours=5, minutes=30)
            now_ist = datetime.now(timezone.utc) + ist_offset
            logger.debug(f"[HOURLY] Current time (IST): {now_ist.strftime('%Y-%m-%d %H:%M')}")

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
                        logger.warning(f"[HOURLY] Could not parse datetime '{raw}' — keeping record")
                        filtered.append(r)
                if skipped:
                    logger.info(f"[HOURLY] Dropped {len(skipped)} past hour record(s)")
                return filtered

            if isinstance(data, dict):
                forecast_key = next((k for k in data if isinstance(data[k], list)), None)
                if forecast_key:
                    logger.debug(f"[HOURLY] Found hourly list under key='{forecast_key}' with {len(data[forecast_key])} records")
                    data[forecast_key] = filter_hourly(data[forecast_key])[:hours]
                    logger.debug(f"[HOURLY] After filter+slice: {len(data[forecast_key])} records")
                else:
                    logger.warning("[HOURLY] No list found inside GFS dict response — returning as-is")
            elif isinstance(data, list):
                data = filter_hourly(data)[:hours]
                logger.debug(f"[HOURLY] After filter+slice: {len(data)} records")

            cache_set(cache_key, data)
            return data

        except httpx.TimeoutException:
            logger.error(f"[HOURLY] ⏱ GFS request timed out for lat={lat}, lon={lon}")
            raise HTTPException(status_code=504, detail="Hourly weather fetch timed out.")
        except httpx.HTTPStatusError as exc:
            logger.error(f"[HOURLY] ❌ GFS HTTP error {exc.response.status_code}: {exc.response.text[:300]}")
            raise HTTPException(status_code=exc.response.status_code, detail=f"Hourly API error: {exc}")
        except Exception as exc:
            logger.error(f"[HOURLY] 💥 Unexpected error: {exc}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Internal server error: {exc}")


# ──────────────────────────────────────────────
# Health check
# ──────────────────────────────────────────────
@app.get("/health")
async def health():
    logger.debug("[HEALTH] Health check called")
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8010)