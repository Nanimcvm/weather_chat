#session.py

import time
from enum import Enum
from typing import Dict, List, Optional

from app.config import SESSION_TTL


class Step(str, Enum):
    IDLE           = "idle"
    COLLECTING     = "collecting"       # asking for missing slots
    DISAMBIGUATING = "disambiguating"   # multiple Solr results, pick one


class SessionState:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self.step       = Step.IDLE

        # ── Set by LLM intent on first message ──────────────────
        self.intent_type  : Optional[str]  = None    # "weather" | "pest"
        self.crop_slug    : Optional[str]  = None
        self.metric       : str            = "ALL"
        self.query_type   : str            = "single"
        self.day_offset   : Optional[int]  = None
        self.target_date  : Optional[str]  = None
        self.range_days   : Optional[int]  = None
        self.hour_range   : Optional[int]  = None
        self.condition    : Optional[str]  = None
        self.is_next_week : bool           = False

        # ── Slots (filled progressively) ────────────────────────
        self.location_str        : Optional[str]        = None   # raw string from user
        self.resolved_location   : Optional[dict]       = None   # Solr doc with coords
        self.location_candidates : Optional[List[dict]] = None   # list when ambiguous
        self.sowing_date         : Optional[str]        = None   # normalised DD-MM-YYYY

        # ── Location memory (persists across queries in the session) ─
        self.last_resolved_location : Optional[dict] = None   # last confirmed Solr doc
        self.last_location_str      : Optional[str]  = None   # human-readable label

        # ── Which slots still need answers ──────────────────────
        self.missing_slots: List[str] = []

        self.history   : List[Dict] = []
        self.created_at  = time.time()
        self.last_active = time.time()

    def touch(self) -> None:
        self.last_active = time.time()

    def remember_location(self) -> None:
        """Call this whenever resolved_location is confirmed. Persists it for future queries."""
        if self.resolved_location:
            self.last_resolved_location = self.resolved_location
            self.last_location_str      = self.location_str

    def log(self, role: str, content: str) -> None:
        self.history.append({"role": role, "content": content, "ts": time.time()})
        if len(self.history) > 20:
            self.history = self.history[-20:]

    def debug(self) -> dict:
        return {
            "session_id":            self.session_id,
            "step":                  self.step,
            "intent_type":           self.intent_type,
            "crop_slug":             self.crop_slug,
            "location_str":          self.location_str,
            "resolved_location":     bool(self.resolved_location),
            "last_location_str":     self.last_location_str,
            "sowing_date":           self.sowing_date,
            "missing_slots":         self.missing_slots,
        }


# ── In-memory session store ───────────────────────────────────
_sessions: Dict[str, SessionState] = {}


def get_session(sid: str) -> SessionState:
    now   = time.time()
    stale = [k for k, v in _sessions.items() if now - v.last_active > SESSION_TTL]
    for k in stale:
        del _sessions[k]
    if sid not in _sessions:
        _sessions[sid] = SessionState(sid)
    s = _sessions[sid]
    s.touch()
    return s


def drop_session(sid: str) -> None:
    _sessions.pop(sid, None)


def active_session_count() -> int:
    return len(_sessions)