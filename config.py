"""Configuration settings."""
import os

# CricketData.org API (legacy/fallback) — set via env var
CRICKETDATA_API_KEY = os.environ.get("CRICKETDATA_API_KEY", "")

# IPL 2026 series ID on CricketData.org
IPL_2026_SERIES_ID = "87c62aac-bc3c-4738-ab93-19da0690488f"

# Cricbuzz API via RapidAPI (primary) — set via env var on Render
CRICBUZZ_API_KEY = os.environ.get("CRICBUZZ_API_KEY", "")

# Cricsheet data directory for ball-by-ball rescoring
CRICSHEET_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "cricsheet")

# Polling intervals (seconds)
LIVE_POLL_INTERVAL = 300       # During match hours (5 min)
IDLE_POLL_INTERVAL = 3600      # Outside match hours (1 hour)

# Cricbuzz RapidAPI monthly call limit (free tier)
CRICBUZZ_MONTHLY_LIMIT = int(os.environ.get("CRICBUZZ_MONTHLY_LIMIT", "80"))

# Match hours in IST (UTC+5:30)
MATCH_START_HOUR_IST = 14
MATCH_END_HOUR_IST = 24

# Database
DB_PATH = os.environ.get("DB_PATH", "data/fantasy.db")

# Admin secret for roster updates
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "adminsecret")

# Season filter
SEASON = os.environ.get("SEASON", "2026")

# GitHub token for remote data backup (set via GITHUB_TOKEN env var on Render)
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
