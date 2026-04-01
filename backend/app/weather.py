from datetime import date as dc, datetime, timedelta, timezone
from typing import Any, Dict

import httpx

from app.config import GFS_HOURLY_URL, GFS_INTERPOLATE_URL
from app.formatters import _err, _intent_dict, _rec_date
from app.session import SessionState

_RANGE_TYPES = {"range", "range_few", "range_week", "conditional_rain", "conditional_condition"}


async def fetch_weather(s: SessionState, sid: str, label: str) -> Dict[str, Any]:
    """
    Call GFS weather APIs and return a structured ``weather_result`` response dict.
    Handles hourly, daily-single, daily-range, and conditional query types.
    """
    lat = s.resolved_location.get("_best_lat")
    lon = s.resolved_location.get("_best_lon")
    if not lat or not lon:
        return _err("Could not resolve coordinates.", sid)

    try:
        hr = 0
        try:
            hr = int(s.hour_range or 0)
        except (TypeError, ValueError):
            pass
        is_hourly = hr > 0 or s.query_type == "hourly"

        async with httpx.AsyncClient(timeout=15.0) as client:
            if is_hourly:
                return await _hourly(client, lat, lon, hr, s, sid, label)
            return await _daily(client, lat, lon, s, sid, label)

    except httpx.TimeoutException:
        return _err("Weather service timed out.", sid)
    except Exception as e:
        return _err(f"Weather fetch failed: {e}", sid)


# ── Internal helpers ──────────────────────────────────────────

async def _hourly(
    client: httpx.AsyncClient,
    lat: Any, lon: Any, hr: int,
    s: SessionState, sid: str, label: str,
) -> Dict[str, Any]:
    r = await client.get(GFS_HOURLY_URL, params={"lat": lat, "lon": lon})
    r.raise_for_status()
    data = r.json()

    ist_now = (
        datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    ).replace(tzinfo=None)

    def filter_future(records):
        out = []
        for rec in records:
            try:
                if datetime.fromisoformat(rec.get("Date_time", "")) >= ist_now:
                    out.append(rec)
            except Exception:
                out.append(rec)
        return out[: hr or 24]

    if isinstance(data, dict):
        fk = next((k for k in data if isinstance(data[k], list)), None)
        if fk:
            data[fk] = filter_future(data[fk])
    else:
        data = filter_future(data)

    return {
        "type":           "weather_result",
        "session_id":     sid,
        "location_label": label,
        "intent":         _intent_dict(s),
        "is_hourly":      True,
        "data":           data,
    }


async def _daily(
    client: httpx.AsyncClient,
    lat: Any, lon: Any,
    s: SessionState, sid: str, label: str,
) -> Dict[str, Any]:
    ist_today = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).date()

    day_offset = None
    if s.day_offset is not None:
        try:
            day_offset = int(s.day_offset)
        except (TypeError, ValueError):
            pass

    res_date   = None
    res_offset = None
    if s.query_type not in _RANGE_TYPES:
        if s.target_date and str(s.target_date) not in ("null", ""):
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
        if qt in ("range", "range_few", "range_week"):
            return from_today(recs)[: (s.range_days or 7)], None
        if qt == "conditional_rain":
            fut   = from_today(recs)[: (s.range_days or len(recs))]
            rainy = [rec for rec in fut if (rec.get("Rainfall") or 0) > 0]
            return (rainy, None) if rainy else ([], "☀️ No rainfall expected.")
        if qt == "conditional_condition":
            _COND = {
                "hot":   lambda rec: rec.get("Tmax",       0) > 38,
                "warm":  lambda rec: rec.get("Tmax",       0) > 30,
                "cold":  lambda rec: rec.get("Tmin",      99) < 15,
                "windy": lambda rec: rec.get("Wind_Speed", 0) > 20,
                "humid": lambda rec: rec.get("RH",         0) > 80,
            }
            fn      = _COND.get(s.condition or "", lambda rec: True)
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

    return {
        "type":           "weather_result",
        "session_id":     sid,
        "location_label": label,
        "intent":         _intent_dict(s),
        "is_hourly":      False,
        "data":           data,
    }