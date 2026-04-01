import json
from typing import Dict

from groq import Groq

from app.config import GROQ_API_KEY

groq_client = Groq(api_key=GROQ_API_KEY)

INTENT_PROMPT = """
You are an intent extraction engine for an agriculture chatbot.
Extract slots from the user query and return STRICT JSON only.

{
  "intent": "weather | pest",
  "is_pest": boolean,
  "location": string | null,
  "crop_slug": string | null,
  "sowing_date": string | null,
  "metric": "ALL | Tmax | Tmin | Tavg | Rainfall | RH | Wind_Speed",
  "query_type": "single | hourly | range | range_week | conditional_rain | conditional_condition | pest_forecast",
  "day_offset": number | null,
  "target_date": "YYYY-MM-DD" | null,
  "range_days": number | null,
  "hour_range": number | null,
  "condition": string | null
}

Rules:
- pest queries (pests/insects/infestation/disease/attack): intent=pest, is_pest=true
- weather queries (rain/temp/humidity/wind/forecast): intent=weather, is_pest=false
- NO date/time mentioned → day_offset=0, query_type=single
- "today", "now", "current", "right now" → day_offset=0
- "tomorrow" → day_offset=1
- "this week", "next 7 days", "weekly" → query_type=range_week
- "next few days", "forecast" → query_type=range, range_days=5
- calendar dates → target_date YYYY-MM-DD, day_offset=null
- sowing dates → DD-MM-YYYY format
- crop name → crop_slug lowercase hyphenated e.g. "paddy", "sugar-cane"
- temperature/temp → metric=Tavg (unless max/min specified)
- max temp / high → metric=Tmax
- min temp / low / cold → metric=Tmin
- rain/rainfall → metric=Rainfall
- humidity → metric=RH
- wind → metric=Wind_Speed
- multiple metrics or general weather → metric=ALL

Return JSON only. No explanation.
"""

_intent_cache: Dict[str, Dict] = {}

_FALLBACK_INTENT: Dict = {
    "intent": "weather", "is_pest": False,
    "location": None, "crop_slug": None, "sowing_date": None,
    "metric": "ALL", "query_type": "single",
    "day_offset": 0, "target_date": None,
    "range_days": None, "hour_range": None, "condition": None,
}

def extract_intent(query: str) -> Dict:
    """Extract structured intent slots from a raw user query via the LLM."""
    if query in _intent_cache:
        return _intent_cache[query]
    try:
        r = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            temperature=0,
            max_tokens=200,
            messages=[
                {"role": "system", "content": INTENT_PROMPT},
                {"role": "user",   "content": query},
            ],
            response_format={"type": "json_object"},
            timeout=10,
        )
        intent = json.loads(r.choices[0].message.content)
        _intent_cache[query] = intent
        return intent
    except Exception:
        return dict(_FALLBACK_INTENT)