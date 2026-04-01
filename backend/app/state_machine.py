"""
Slot-filling state machine shared by both the /api/chat and /api/unified_chat endpoints.

Public surface:
    _missing(s)                   -> List[str]
    _ask(s, sid, errors)          -> Dict  (ask_slots response)
    _collect(s, message, sid)     -> Dict  (async, processes free-text answer)
    _fill_slots(s, slots, sid)    -> Dict  (async, processes structured slot dict)
    _resolve_and_call(s, sid)     -> Dict  (async, resolve location then call API)
    _call_api(s, sid)             -> Dict  (async, call weather or pest API)
"""

from typing import Any, Dict, List, Optional

from app.formatters import _err, _intent_dict
from app.location import loc_label, solr_search
from app.pest import fetch_pest
from app.session import SessionState, Step
from app.validators import extract_date, normalise_date, valid_location_str
from app.weather import fetch_weather


# ── Slot introspection ────────────────────────────────────────

def _missing(s: SessionState) -> List[str]:
    """
    Return slot names that are still required but not yet filled.

    Location memory: if the user did not mention a location in the new query
    but we already confirmed one earlier in this session, reuse it silently
    instead of asking again.
    """
    out: List[str] = []
    if not s.location_str:
        if s.last_resolved_location:
            # Reuse the location remembered from a previous query
            s.resolved_location = s.last_resolved_location
            s.location_str      = s.last_location_str
        else:
            out.append("location")
    if s.intent_type == "pest" and not s.crop_slug:
        out.append("crop")
    if s.intent_type == "pest" and not s.sowing_date:
        out.append("sowing_date")
    return out


# ── Ask card builder ──────────────────────────────────────────

_FIELD_META = {
    "location":    {"label": "Location",    "hint": "e.g. Hyderabad, Guntur, Vijayawada"},
    "crop":        {"label": "Crop",        "hint": "e.g. paddy, potato, blackgram"},
    "sowing_date": {"label": "Sowing Date", "hint": "DD-MM-YYYY  e.g. 10-12-2025"},
}


def _ask(
    s: SessionState,
    sid: str,
    errors: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Build an ask_slots response card for every slot still in s.missing_slots.
    errors maps slot names to inline error strings.
    """
    fields = []
    for slot in s.missing_slots:
        f = {"slot": slot, **_FIELD_META.get(slot, {"label": slot, "hint": ""})}
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


# ── Free-text collection ──────────────────────────────────────

async def _collect(s: SessionState, message: str, sid: str) -> Dict[str, Any]:
    """
    Called when s.step == Step.COLLECTING and the user sent a free-text reply.
    Fills as many missing slots as possible, then either re-asks or calls the API.
    """
    errors: Dict[str, str] = {}

    # Pass 1: non-location slots (so they are saved before any disambiguation redirect)
    for slot in list(s.missing_slots):
        if slot == "crop":
            crop_val = message.strip().lower().replace(" ", "-")
            if len(crop_val) >= 2:
                s.crop_slug     = crop_val
                s.missing_slots = [m for m in s.missing_slots if m != "crop"]
            else:
                errors["crop"] = "Please enter a valid crop name (e.g. paddy, wheat, cotton)."

        elif slot == "sowing_date":
            extracted = extract_date(message)
            if not extracted:
                errors["sowing_date"] = (
                    "Please enter a valid sowing date in DD-MM-YYYY format (e.g. 10-12-2025)."
                )
            else:
                s.sowing_date   = normalise_date(extracted)
                s.missing_slots = [m for m in s.missing_slots if m != "sowing_date"]

    # Pass 2: location (may redirect to disambiguation)
    if "location" in s.missing_slots:
        result = await _resolve_location_from_text(s, message, sid)
        if result is not None:
            return result

    # Recompute missing after all passes
    s.missing_slots = _missing(s)

    if s.missing_slots or errors:
        s.step = Step.COLLECTING
        return _ask(s, sid, errors if errors else None)

    return await _call_api(s, sid)


# ── Structured slot fill (from chip/form submission) ─────────

async def _fill_slots(
    s: SessionState,
    slots: Dict[str, str],
    sid: str,
) -> Dict[str, Any]:
    """
    Fill multiple slots at once from a structured dict (e.g. a submitted form).
    Validates each value; returns disambiguation card or calls the API when done.
    """
    errors: Dict[str, str] = {}

    # Fill crop
    if "crop" in slots:
        crop_val = slots["crop"].strip().lower().replace(" ", "-")
        if len(crop_val) >= 2:
            s.crop_slug     = crop_val
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
            s.sowing_date   = normalise_date(extracted)
            s.missing_slots = [m for m in s.missing_slots if m != "sowing_date"]

    # Fill location last — may trigger disambiguation
    if "location" in slots:
        candidate = slots["location"].strip()
        if not valid_location_str(candidate):
            errors["location"] = "Please enter a valid location name (district or state)."
        else:
            result = await _resolve_location(s, candidate, sid)
            if result is not None:
                # Disambiguation or error — crop/sowing already saved above
                return result

    # Recompute
    s.missing_slots = _missing(s)

    if errors or s.missing_slots:
        s.step = Step.COLLECTING
        return _ask(s, sid, errors if errors else None)

    return await _call_api(s, sid)


# ── Location resolution + disambiguation ─────────────────────

async def _resolve_and_call(s: SessionState, sid: str) -> Dict[str, Any]:
    """
    s.location_str is known but coords are not yet resolved.
    Search Solr, handle ambiguity, then call the API.
    """
    try:
        docs = await solr_search(s.location_str)
    except Exception as e:
        s.step = Step.IDLE
        return _err(f"Location search failed: {e}", sid)

    if not docs:
        s.location_str  = None
        s.missing_slots = _missing(s)
        s.step          = Step.COLLECTING
        return _ask(
            s, sid,
            errors={"location": f"<strong>{s.location_str or 'That location'}</strong> wasn't found."},
        )

    if len(docs) == 1:
        s.resolved_location = docs[0]
        s.remember_location()
        return await _call_api(s, sid)

    # Try exact match against the user's input before showing disambiguation
    match = _exact_match(docs, s.location_str)
    if match:
        s.resolved_location = match
        s.location_str      = loc_label(match)
        s.remember_location()
        return await _call_api(s, sid)

    s.location_candidates = docs
    s.step = Step.DISAMBIGUATING
    return _disambiguation_card(docs, sid)


async def _call_api(s: SessionState, sid: str) -> Dict[str, Any]:
    """
    Entry point for firing the downstream API once all slots are filled.
    Resolves Solr coords if not yet done, then dispatches to weather or pest.
    """
    if s.resolved_location is None and s.location_str:
        try:
            docs = await solr_search(s.location_str)
        except Exception as e:
            s.step = Step.IDLE
            return _err(f"Location search failed: {e}", sid)

        if not docs:
            s.location_str  = None
            s.missing_slots = ["location"]
            s.step          = Step.COLLECTING
            return _ask(
                s, sid,
                errors={"location": "Could not find the location. Please enter a valid district or state."},
            )

        if len(docs) == 1:
            s.resolved_location = docs[0]
            s.remember_location()
        else:
            # Try exact match before showing disambiguation
            match = _exact_match(docs, s.location_str)
            if match:
                s.resolved_location = match
                s.location_str      = loc_label(match)
                s.remember_location()
            else:
                s.location_candidates = docs
                s.missing_slots       = ["location"]
                s.step                = Step.DISAMBIGUATING
                return _disambiguation_card(docs, sid)

    if s.resolved_location is None:
        s.missing_slots = ["location"]
        s.step          = Step.COLLECTING
        return _ask(s, sid)

    # Persist the confirmed location for future queries in this session
    s.remember_location()

    s.step = Step.IDLE  # ready for next query
    label = loc_label(s.resolved_location, fallback=s.location_str or "Unknown location")

    if s.intent_type == "pest":
        return await fetch_pest(s, sid, label)
    return await fetch_weather(s, sid, label)


# ── Private utilities ─────────────────────────────────────────


def _exact_match(docs: list, query: str) -> Optional[dict]:
    """
    Return the single doc whose label exactly matches the query string.
    Also returns a doc if only ONE doc matches the query as a substring.
    Returns None when genuinely ambiguous (multiple matches or no match).
    """
    if not query:
        return None
    clean_query = query.lower().replace(".", "").replace(",", "").strip()
    exact = [d for d in docs if loc_label(d).lower().replace(".", "").replace(",", "").strip() == clean_query]
    if len(exact) == 1:
        return exact[0]
    # Fallback: query is contained in label (handles "Hyderabad, Telangana" vs fuller names)
    partial = [d for d in docs if clean_query in loc_label(d).lower().replace(".", "").replace(",", "").strip()]
    if len(partial) == 1:
        return partial[0]
    return None

async def _resolve_location_from_text(
    s: SessionState, text: str, sid: str
) -> Optional[Dict[str, Any]]:
    """
    Try to resolve text as a location.  Returns a response dict only when
    something went wrong (not found, ambiguous); returns None when the slot
    was filled successfully so the caller can continue.
    """
    candidate = text.strip()
    if not valid_location_str(candidate):
        return None   # let caller add the error

    return await _resolve_location(s, candidate, sid)


async def _resolve_location(
    s: SessionState, candidate: str, sid: str
) -> Optional[Dict[str, Any]]:
    """
    Look up candidate in Solr and update s accordingly.
    Returns a response dict on disambiguation/error, or None on clean success.
    Calls s.remember_location() on every clean resolution so the location is
    available for follow-up queries even without an explicit mention.
    """
    try:
        docs = await solr_search(candidate)
    except Exception as e:
        return _ask(
            s, sid,
            errors={"location": f"Location lookup failed: {e}"},
        )

    if not docs:
        return _ask(
            s, sid,
            errors={
                "location": (
                    f"<strong>{candidate}</strong> wasn't found. "
                    "Please enter a valid district or state name."
                )
            },
        )

    if len(docs) == 1:
        s.resolved_location = docs[0]
        s.location_str      = loc_label(s.resolved_location)
        s.missing_slots     = [m for m in s.missing_slots if m != "location"]
        s.remember_location()
        return None   # success

    # Try exact match before showing disambiguation
    match = _exact_match(docs, candidate)
    if match:
        s.resolved_location = match
        s.location_str      = loc_label(match)
        s.missing_slots     = [m for m in s.missing_slots if m != "location"]
        s.remember_location()
        return None   # success

    # Genuinely ambiguous — user must pick
    s.location_candidates = docs
    s.location_str        = candidate
    s.missing_slots       = [m for m in s.missing_slots if m != "location"]
    s.step                = Step.DISAMBIGUATING
    return _disambiguation_card(docs, sid)


def _disambiguation_card(docs: list, sid: str) -> Dict[str, Any]:
    return {
        "type":       "ask_location_choice",
        "session_id": sid,
        "candidates": [
            {"index": i, "label": loc_label(d)}
            for i, d in enumerate(docs)
        ],
    }