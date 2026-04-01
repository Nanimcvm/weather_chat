from datetime import datetime
from typing import Any, Dict, Optional

from app.session import SessionState


# ── Generic response helpers ──────────────────────────────────

def _err(message: str, sid: str) -> Dict[str, Any]:
    return {"type": "error", "session_id": sid, "message": message}


def _intent_dict(s: SessionState) -> Dict[str, Any]:
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


def _rec_date(r: dict):
    """Parse a record's date field for comparison.  Returns ``datetime.min.date()`` on failure."""
    try:
        return datetime.fromisoformat(r.get("Date_time", r.get("Date", ""))).date()
    except Exception:
        return datetime.min.date()


# ── Unified-API human-readable answer formatter ───────────────

def _safe_round(val: Any) -> Any:
    try:
        return round(float(val), 2)
    except Exception:
        return val


def format_final_answer(s: SessionState, data: Any, label: str) -> str:
    """
    Convert raw API data into a single human-readable sentence.
    Used by the unified chat endpoint.
    """
    if s.intent_type == "pest":
        return _format_pest_answer(s, data, label)
    return _format_weather_answer(s, data, label)


def _format_pest_answer(s: SessionState, data: Any, label: str) -> str:
    infestations = data.get("data", []) if isinstance(data, dict) else []
    if not infestations:
        return (
            f"There are no particular pest risks detected for "
            f"{s.crop_slug} in {label} right now."
        )
    risks = [
        f"{r.get('infestation_name')} "
        f"({r.get('chances_percentage', {}).get('current_week', 0)}%)"
        for r in infestations
        if r.get("chances_percentage", {}).get("current_week", 0) > 0
    ]
    if risks:
        return f"The pest risks for {s.crop_slug} in {label} are: " + ", ".join(risks)
    return f"No significant pest risks for {s.crop_slug} in {label}."


def _format_weather_answer(s: SessionState, data: Any, label: str) -> str:
    records = (
        data.get("Forecast data", data.get("weather_data", []))
        if isinstance(data, dict)
        else data
    )
    if isinstance(records, dict):
        records = [records]
    if not records or not isinstance(records, list) or len(records) == 0:
        return f"Could not find weather data for {label}."

    r   = records[0]
    day = (
        "today"
        if s.day_offset == 0
        else f"on {s.target_date}"
        if s.target_date
        else "for the requested time"
    )
    m = s.metric
    if m == "Tmax":
        return f"The maximum temperature in {label} {day} is {_safe_round(r.get('Tmax', 0))}°C."
    if m == "Tmin":
        return f"The minimum temperature in {label} {day} is {_safe_round(r.get('Tmin', 0))}°C."
    if m in ("Tavg", "Temp"):
        return f"The temperature in {label} {day} is {_safe_round(r.get('Tavg', r.get('Tmax', 0)))}°C."
    if m == "Rainfall":
        return f"The expected rainfall in {label} {day} is {_safe_round(r.get('Rainfall', 0))} mm."
    if m in ("RH", "Humidity"):
        return f"The humidity in {label} {day} is {_safe_round(r.get('RH', 0))}%."
    if m == "Wind_Speed":
        return f"The wind speed in {label} {day} is {_safe_round(r.get('Wind_Speed', 0))} km/h."

    return (
        f"The weather in {label} {day} is "
        f"{_safe_round(r.get('Tavg', r.get('Tmax', 0)))}°C, "
        f"with {_safe_round(r.get('Rainfall', 0))} mm rainfall "
        f"and {_safe_round(r.get('RH', 0))}% humidity."
    )