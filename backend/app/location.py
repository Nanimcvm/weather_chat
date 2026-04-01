import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.config import CACHE_TTL, SOLR_AUTH, SOLR_SEARCH_URL

# ── Simple TTL cache ──────────────────────────────────────────
_cache:    Dict[str, Any]   = {}
_cache_ts: Dict[str, float] = {}


def _cget(k: str) -> Optional[Any]:
    if k in _cache and time.time() - _cache_ts[k] < CACHE_TTL:
        return _cache[k]
    return None


def _cset(k: str, v: Any) -> None:
    _cache[k]    = v
    _cache_ts[k] = time.time()


# ── Coordinate field priority ─────────────────────────────────
_COORD_PRIORITY = [
    ("village_latitude",    "village_longitude"),
    ("village_lat",         "village_lon"),
    ("mandal_latitude",     "mandal_longitude"),
    ("taluk_latitude",      "taluk_longitude"),
    ("subdistrict_latitude","subdistrict_longitude"),
    ("block_latitude",      "block_longitude"),
    ("district_latitude",   "district_longitude"),
    ("district_lat",        "district_lon"),
    ("state_latitude",      "state_longitude"),
    ("state_lat",           "state_lon"),
]


def best_coords(doc: dict) -> Tuple[Optional[Any], Optional[Any]]:
    """Return the finest-grained (lat, lon) pair available in a Solr doc."""
    for lf, lo in _COORD_PRIORITY:
        lat = doc.get(lf)
        lon = doc.get(lo)
        if lat and lon:
            lat = lat[0] if isinstance(lat, list) else lat
            lon = lon[0] if isinstance(lon, list) else lon
            if lat and lon:
                return lat, lon
    return None, None


def _unwrap(val: Any) -> Optional[str]:
    """Unwrap a Solr array field to a plain string."""
    if isinstance(val, list):
        return val[0] if val else None
    return val


def loc_label(doc: Optional[dict], fallback: str = "Unknown location") -> str:
    """Build a human-readable 'Village/District, State' label from a Solr doc."""
    if doc is None:
        return fallback
    name  = _unwrap(doc.get("village")) or _unwrap(doc.get("district")) or _unwrap(doc.get("state"))
    state = _unwrap(doc.get("state"))
    parts = [name, state] if name != state else [name]
    return ", ".join(p for p in parts if p) or fallback


async def solr_search(q: str) -> List[dict]:
    """
    Search Solr for the given location string.
    Results are cached for CACHE_TTL seconds.
    Each returned doc is augmented with ``_best_lat`` / ``_best_lon``.
    """
    key   = f"solr:{q.lower().strip()}"
    cached = _cget(key)
    if cached is not None:
        return cached

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