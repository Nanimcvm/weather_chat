import re
from typing import Optional


def valid_location_str(s: str) -> bool:
    """Return True if s looks like a real place name."""
    return bool(s) and len(s.strip()) >= 2 and not re.fullmatch(r"[\W\d\s]+", s.strip())


def valid_sowing_date(s: str) -> bool:
    """Accept DD-MM-YYYY or YYYY-MM-DD."""
    return bool(
        re.match(r"^\d{2}-\d{2}-\d{4}$", s)
        or re.match(r"^\d{4}-\d{2}-\d{2}$", s)
    )


def normalise_date(s: str) -> str:
    """Convert YYYY-MM-DD → DD-MM-YYYY.  DD-MM-YYYY is returned unchanged."""
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        y, m, d = s.split("-")
        return f"{d}-{m}-{y}"
    return s


_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4,
    "june": 6, "july": 7, "august": 8, "september": 9,
    "october": 10, "november": 11, "december": 12,
}


def extract_date(text: str) -> Optional[str]:
    """
    Pull a date out of free text.
    Returns DD-MM-YYYY or None.
    """
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
    m = re.search(r"\b(\d{1,2})\s+([a-zA-Z]+)\s+(\d{4})\b", text)
    if m and m.group(2).lower() in _MONTHS:
        return (
            f"{m.group(1).zfill(2)}-"
            f"{str(_MONTHS[m.group(2).lower()]).zfill(2)}-"
            f"{m.group(3)}"
        )

    m = re.search(r"\b([a-zA-Z]+)\s+(\d{1,2})\s+(\d{4})\b", text)
    if m and m.group(1).lower() in _MONTHS:
        return (
            f"{m.group(2).zfill(2)}-"
            f"{str(_MONTHS[m.group(1).lower()]).zfill(2)}-"
            f"{m.group(3)}"
        )

    return None