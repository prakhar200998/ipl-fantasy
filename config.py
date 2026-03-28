"""Configuration settings."""
import os

# CricketData.org API
CRICKETDATA_API_KEY = os.environ.get("CRICKETDATA_API_KEY", "")

# Polling intervals (seconds)
LIVE_POLL_INTERVAL = 60        # During match hours
IDLE_POLL_INTERVAL = 1800      # Outside match hours (30 min)

# Match hours in IST (UTC+5:30)
MATCH_START_HOUR_IST = 14
MATCH_END_HOUR_IST = 24

# Database
DB_PATH = os.environ.get("DB_PATH", "data/fantasy.db")

# Cricsheet data directory (for historical/testing)
CRICSHEET_DATA_DIR = os.environ.get("CRICSHEET_DATA_DIR", "/Users/prakharbhandari/Desktop/ipl_json")

# Admin secret for roster updates
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "changeme")

# Season filter
SEASON = os.environ.get("SEASON", "2025")
