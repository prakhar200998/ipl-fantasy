"""Configuration settings."""
import os

# CricketData.org API
CRICKETDATA_API_KEY = os.environ.get("CRICKETDATA_API_KEY", "67ece779-66b8-43ef-84a7-a6ae6a0afff3")

# IPL 2026 series ID on CricketData.org
IPL_2026_SERIES_ID = "87c62aac-bc3c-4738-ab93-19da0690488f"

# Polling intervals (seconds)
LIVE_POLL_INTERVAL = 300       # During match hours (5 min — free tier has 100 calls/day)
IDLE_POLL_INTERVAL = 3600      # Outside match hours (1 hour)

# Match hours in IST (UTC+5:30)
MATCH_START_HOUR_IST = 14
MATCH_END_HOUR_IST = 24

# Database
DB_PATH = os.environ.get("DB_PATH", "data/fantasy.db")

# Admin secret for roster updates
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "adminsecret")

# Season filter
SEASON = os.environ.get("SEASON", "2026")
