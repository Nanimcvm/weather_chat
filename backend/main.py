from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
import re
from typing import Optional, List, Dict

app = FastAPI(title="WeatherSnap ChatBot Backend")

# NLU Helpers
STOP_WORDS = {"what", "is", "today", "the", "weather", "forecast", "in", "at", "for", "of", "how", "max", "min", "temperature", "temp", "rain", "rainfall", "humidity", "wind", "speed", "climate", "show", "me", "look", "like"}
METRIC_MAP = {
    "max temperature": "Tmax",
    "maximum temperature": "Tmax",
    "max temp": "Tmax",
    "min temperature": "Tmin",
    "minimum temperature": "Tmin",
    "min temp": "Tmin",
    "avg temperature": "Tavg",
    "average temperature": "Tavg",
    "rain": "Rainfall",
    "rainfall": "Rainfall",
    "humidity": "RH",
    "wind": "Wind_Speed",
    "wind speed": "Wind_Speed"
}

def extract_intent(query: str) -> Dict:
    query_clean = query.lower().strip()
    
    # Extract Metric
    metric = None
    for key, value in METRIC_MAP.items():
        if key in query_clean:
            metric = value
            break
            
    # Extract Location (Heuristic: after 'in', 'at', 'for' or remaining words)
    location = query_clean
    patterns = [r"in\s+([a-zA-Z\s]+)", r"at\s+([a-zA-Z\s]+)", r"for\s+([a-zA-Z\s]+)"]
    for pattern in patterns:
        match = re.search(pattern, query_clean)
        if match:
            location = match.group(1).strip()
            break
    
    if location == query_clean:
        # Simple cleanup if no pattern matched
        words = query_clean.split()
        location_words = [w for w in words if w not in STOP_WORDS]
        location = " ".join(location_words).strip()
    
    return {
        "location": location or query_clean,
        "metric": metric
    }

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SOLR_SEARCH_URL = "https://solr.apps.niruthi.com/solr/location_data/select"
SOLR_AUTH = "Basic YXBpOk5pcnV0aGlAMjRVc2Vy"
GFS_INTERPOLATE_URL = "https://gfsapi.niruthiapptesting.com/interpolate"
GFS_HOURLY_URL = "https://gfsapi.niruthiapptesting.com/hrlydata"

@app.get("/api/search")
async def search_location(q: str = Query(...)):
    """
    Search for locations using Solr API with NLP preprocessing
    """
    intent = extract_intent(q)
    extracted_q = intent["location"]
    
    params = {
        "q": f"(village:{extracted_q}~* OR state:{extracted_q}~* OR district:{extracted_q}~*)",
        "rows": 8,
        "wt": "json"
    }
    headers = {"Authorization": SOLR_AUTH}
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(SOLR_SEARCH_URL, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            # Inject intent into response for frontend
            if "response" in data:
                data["intent"] = intent
            return data
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=f"Search API error: {exc}")
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Internal server error: {exc}")

@app.get("/api/weather/daily")
async def get_daily_weather(lat: float, lon: float):
    """
    Fetch daily weather forecast
    """
    params = {"lat": lat, "lon": lon}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(GFS_INTERPOLATE_URL, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=f"Interpolate API error: {exc}")
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Internal server error: {exc}")

@app.get("/api/weather/hourly")
async def get_hourly_weather(lat: float, lon: float):
    """
    Fetch hourly weather forecast
    """
    params = {"lat": lat, "lon": lon}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(GFS_HOURLY_URL, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=f"Hourly API error: {exc}")
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Internal server error: {exc}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8010)
