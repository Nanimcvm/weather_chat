from typing import Any, Dict, Optional

import httpx

from app.config import GFS_INFESTATION_URL
from app.formatters import _err, _intent_dict
from app.location import _unwrap
from app.session import SessionState
from app.validators import normalise_date

# Reuse the same TTL cache from location module
from app.location import _cget, _cset


async def fetch_pest(s: SessionState, sid: str, label: str) -> Dict[str, Any]:
    """
    Call the GFS infestation API and return a structured ``pest_result`` response dict.
    Results are cached to avoid duplicate calls for the same parameters.
    """
    lat = s.resolved_location.get("_best_lat")
    lon = s.resolved_location.get("_best_lon")
    if not lat or not lon:
        return _err("Could not resolve coordinates.", sid)

    state: Optional[str] = s.resolved_location.get("state")
    if isinstance(state, list):
        state = state[0]

    sowing = normalise_date(s.sowing_date)
    crop   = s.crop_slug
    key    = f"pest:{lat}:{lon}:{s.is_next_week}:{sowing}:{crop}:{state}"

    if (cached := _cget(key)):
        return {
            "type":           "pest_result",
            "session_id":     sid,
            "location_label": label,
            "state":          state,
            "is_next_week":   s.is_next_week,
            "intent":         _intent_dict(s),
            "data":           cached,
        }

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                GFS_INFESTATION_URL,
                params={
                    "lat":          lat,
                    "lon":          lon,
                    "is_next_week": str(s.is_next_week).lower(),
                },
                json={
                    "sowing_date": sowing,
                    "crop_slug":   crop,
                    "state_name":  state,
                },
            )
            r.raise_for_status()
            data = r.json()

        _cset(key, data)
        return {
            "type":           "pest_result",
            "session_id":     sid,
            "location_label": label,
            "state":          state,
            "is_next_week":   s.is_next_week,
            "intent":         _intent_dict(s),
            "data":           data,
        }

    except httpx.TimeoutException:
        return _err("Pest service timed out.", sid)
    except Exception as e:
        return _err(f"Pest fetch failed: {e}", sid)