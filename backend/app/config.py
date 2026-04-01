import os
from dotenv import load_dotenv

load_dotenv()

# ── Groq ──────────────────────────────────────────────────────
GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")

# ── LangSmith tracing ─────────────────────────────────────────
os.environ["LANGCHAIN_TRACING_V2"]  = os.getenv("LANGCHAIN_TRACING_V2", "true")
os.environ["LANGCHAIN_API_KEY"]     = os.getenv("LANGSMITH_API_KEY", "")
os.environ["LANGCHAIN_PROJECT"]     = os.getenv("LANGSMITH_PROJECT", "agribot")
os.environ["LANGCHAIN_ENDPOINT"]    = os.getenv("LANGCHAIN_ENDPOINT", "https://api.smith.langchain.com")

# ── External service URLs ─────────────────────────────────────
SOLR_SEARCH_URL     : str = os.getenv("SOLR_SEARCH_URL", "")
SOLR_AUTH           : str = os.getenv("SOLR_AUTH_TOKEN", "")
GFS_INTERPOLATE_URL : str = os.getenv("GFS_INTERPOLATE_URL", "")
GFS_HOURLY_URL      : str = os.getenv("GFS_HOURLY_URL", "")
GFS_INFESTATION_URL : str = os.getenv("GFS_INFESTATION_URL", "")

# ── CORS ──────────────────────────────────────────────────────
ALLOWED_ORIGINS: list[str] = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# ── Session ───────────────────────────────────────────────────
SESSION_TTL: int = 1800   # 30 minutes inactivity

# ── Cache ─────────────────────────────────────────────────────
CACHE_TTL: int = 300      # 5 minutes