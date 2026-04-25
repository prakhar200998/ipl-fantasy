"""Configuration settings."""
import os

# CricketData.org API (primary) — set via env var
CRICKETDATA_API_KEY = os.environ.get("CRICKETDATA_API_KEY", "")

# IPL 2026 series ID on CricketData.org
IPL_2026_SERIES_ID = "87c62aac-bc3c-4738-ab93-19da0690488f"

# Cricbuzz API via RapidAPI (dormant — available via admin) — set via env var on Render
CRICBUZZ_API_KEY = os.environ.get("CRICBUZZ_API_KEY", "")

# Cricsheet data directory for ball-by-ball rescoring
CRICSHEET_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "cricsheet")

# CricketData.org daily call limit (free tier: 100/day, we cap at 90 for safety)
CRICKETDATA_DAILY_LIMIT = int(os.environ.get("CRICKETDATA_DAILY_LIMIT", "90"))

# Cricbuzz RapidAPI monthly call limit (free tier: 200/month)
CRICBUZZ_MONTHLY_LIMIT = int(os.environ.get("CRICBUZZ_MONTHLY_LIMIT", "200"))

# Database
DB_PATH = os.environ.get("DB_PATH", "data/fantasy.db")

# Admin secret for roster updates
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "adminsecret")

# Season filter
SEASON = os.environ.get("SEASON", "2026")

# GitHub token for remote data backup (set via GITHUB_TOKEN env var on Render)
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")

# Mid-season auction cutoff. Matches with date < cutoff = Phase 1 (frozen).
# Matches with date >= cutoff = Phase 2 (live, attributed to new rosters).
PHASE2_CUTOFF_DATE = "2026-04-25"
