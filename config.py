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

# Cricbuzz RapidAPI monthly call limit (free tier: 200/month)
CRICBUZZ_MONTHLY_LIMIT = int(os.environ.get("CRICBUZZ_MONTHLY_LIMIT", "200"))

# Scheduled fetch times in IST (hour, minute)
# Afternoon match slots (double-header days):
#   3:45 PM — mid 1st innings, 5:00 PM — end 1st innings
# Evening match slots (every match day):
#   7:30 PM — start, 8:30 PM — mid 1st, 9:30 PM — end 1st,
#   10:30 PM — mid 2nd, 11:30 PM — match end
FETCH_TIMES_IST = [
    (15, 45),
    (17, 0),
    (19, 30),
    (20, 30),
    (21, 30),
    (22, 30),
    (23, 30),
]

# Database
DB_PATH = os.environ.get("DB_PATH", "data/fantasy.db")

# Admin secret for roster updates
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "adminsecret")

# Season filter
SEASON = os.environ.get("SEASON", "2026")

# GitHub token for remote data backup (set via GITHUB_TOKEN env var on Render)
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
