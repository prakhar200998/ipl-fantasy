"""FastAPI app — routes, startup, background poller.

Data-source strategy:
  Primary (live + completed): Cricbuzz API via RapidAPI
  Accuracy pass (completed):  Cricsheet ball-by-ball (has dot balls)
  Legacy fallback:            CricketData.org API
"""
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.background import BackgroundScheduler

import database as db
from adapters.cricbuzz import CricbuzzAdapter
from adapters.cricsheet import (
    CricsheetAdapter, download_cricsheet_ipl, find_cricsheet_match_id,
)
from scoring import calculate_fantasy_points
from name_mapping import get_display_name
from teams import get_captain_vc
from config import (
    CRICBUZZ_API_KEY, CRICSHEET_DATA_DIR, SEASON,
    LIVE_POLL_INTERVAL, IDLE_POLL_INTERVAL, ADMIN_SECRET,
    MATCH_START_HOUR_IST, MATCH_END_HOUR_IST,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
scheduler = BackgroundScheduler()


def is_match_hours() -> bool:
    now_ist = datetime.now(IST)
    return MATCH_START_HOUR_IST <= now_ist.hour < MATCH_END_HOUR_IST


# ------------------------------------------------------------------
# Data fetching — Cricbuzz primary
# ------------------------------------------------------------------

def fetch_and_store_completed_matches():
    """Fetch completed IPL 2026 matches from Cricbuzz and store points."""
    if not CRICBUZZ_API_KEY:
        logger.warning("No CRICBUZZ_API_KEY set — skipping fetch")
        return

    adapter = CricbuzzAdapter()
    matches = adapter.get_match_list(SEASON)
    completed = [m for m in matches if m["status"] == "complete"]
    logger.info("Cricbuzz: found %d completed IPL 2026 matches", len(completed))

    # Skip matches already stored as complete
    conn = db.get_db()
    stored_complete = {
        row["match_id"]
        for row in conn.execute(
            "SELECT match_id FROM matches WHERE status = 'complete'"
        ).fetchall()
    }
    conn.close()

    new_matches = [m for m in completed if m["match_id"] not in stored_complete]
    if not new_matches:
        logger.info("All completed matches already stored")
        return

    captain_vc = get_captain_vc()

    for match in new_matches:
        scorecard = adapter.get_scorecard(match["match_id"])
        if not scorecard:
            logger.warning("No scorecard for match %s", match.get("name", match["match_id"]))
            continue
        points = calculate_fantasy_points(scorecard)
        db.upsert_match(
            match["match_id"], match["date"], match["teams"],
            match["venue"], "complete",
        )
        db.bulk_upsert_player_points(match["match_id"], points, captain_vc)
        logger.info(
            "Stored match %s: %s (from Cricbuzz, dots=0)",
            match["match_id"], match.get("name", ""),
        )


def poll_live_matches():
    """Background job: fetch live/recently completed match data via Cricbuzz."""
    if not CRICBUZZ_API_KEY:
        return
    try:
        adapter = CricbuzzAdapter()

        # During match hours: check for live matches
        # Outside match hours: check for recently completed matches
        matches = adapter.get_match_list(SEASON)
        active = [m for m in matches if m["status"] in ("in_progress", "complete")]

        if not active:
            return

        # Skip matches already stored as complete
        conn = db.get_db()
        stored = {
            row["match_id"]: row["status"]
            for row in conn.execute(
                "SELECT match_id, status FROM matches"
            ).fetchall()
        }
        conn.close()

        captain_vc = get_captain_vc()

        for match in active:
            # Skip if already stored as complete
            if (stored.get(match["match_id"]) == "complete"
                    and match["status"] == "complete"):
                continue

            scorecard = adapter.get_scorecard(match["match_id"])
            if not scorecard:
                continue

            points = calculate_fantasy_points(scorecard)
            db.upsert_match(
                match["match_id"], match["date"], match["teams"],
                match["venue"], match["status"],
            )
            db.bulk_upsert_player_points(match["match_id"], points, captain_vc)
            logger.info(
                "Updated match %s (status=%s, source=cricbuzz)",
                match["match_id"], match["status"],
            )

    except Exception as e:
        logger.error("Poll error: %s", e)


# ------------------------------------------------------------------
# Cricsheet re-scoring — adds dot-ball accuracy to completed matches
# ------------------------------------------------------------------

def rescore_from_cricsheet():
    """Download Cricsheet ball-by-ball data and re-score completed matches.

    This gives us accurate dot-ball counts (worth 2 pts each) and
    precise maiden/over-level data that the Cricbuzz scorecard API
    doesn't provide.
    """
    try:
        if not download_cricsheet_ipl(CRICSHEET_DATA_DIR):
            return

        cs_adapter = CricsheetAdapter(CRICSHEET_DATA_DIR)
        captain_vc = get_captain_vc()

        # Get all completed matches from our DB
        conn = db.get_db()
        db_matches = conn.execute(
            "SELECT match_id, date, teams_json FROM matches WHERE status = 'complete'"
        ).fetchall()
        conn.close()

        if not db_matches:
            logger.info("No completed matches to re-score from Cricsheet")
            return

        rescored = 0
        for row in db_matches:
            import json
            teams = json.loads(row["teams_json"]) if row["teams_json"] else []
            date = row["date"] or ""

            cs_match_id = find_cricsheet_match_id(
                cs_adapter, SEASON, date, teams,
            )
            if not cs_match_id:
                continue

            scorecard = cs_adapter.get_scorecard(cs_match_id)
            if not scorecard:
                continue

            points = calculate_fantasy_points(scorecard)
            # Re-store with the ORIGINAL DB match_id (Cricbuzz ID)
            db.bulk_upsert_player_points(row["match_id"], points, captain_vc)
            rescored += 1
            logger.info(
                "Re-scored match %s from Cricsheet (with dots)",
                row["match_id"],
            )

        if rescored:
            logger.info("Cricsheet re-scoring complete: %d matches updated", rescored)
        else:
            logger.info("No new Cricsheet data available for re-scoring")

    except Exception as e:
        logger.error("Cricsheet re-score error: %s", e)


# ------------------------------------------------------------------
# App lifecycle
# ------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()

    # Check if teams are seeded
    conn = db.get_db()
    team_count = conn.execute("SELECT COUNT(*) as cnt FROM teams").fetchone()["cnt"]
    conn.close()

    if team_count == 0:
        logger.info("No teams in DB — seeding teams...")
        from teams import TEAMS
        db.seed_teams(TEAMS)

    # Fetch completed matches from Cricbuzz
    if CRICBUZZ_API_KEY:
        try:
            fetch_and_store_completed_matches()
        except Exception as e:
            logger.error("Startup fetch error (will retry via poller): %s", e)

    logger.info("DB has %d matches", db.get_match_count())

    # Start background scheduler
    if CRICBUZZ_API_KEY:
        interval = LIVE_POLL_INTERVAL if is_match_hours() else IDLE_POLL_INTERVAL
        scheduler.add_job(
            poll_live_matches, "interval",
            seconds=interval, id="poller",
        )
        # Cricsheet re-scoring: run once 30s after startup, then every 2 hours
        scheduler.add_job(
            rescore_from_cricsheet, "interval",
            hours=2, id="cricsheet_rescore",
            next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
        scheduler.start()
        logger.info("Scheduler started (poll=%ds, cricsheet every 2h)", interval)
    else:
        logger.info("No API key set — live polling disabled")

    yield

    if scheduler.running:
        scheduler.shutdown()


app = FastAPI(title="IPL Fantasy League", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/standings")
async def standings():
    data = db.get_standings()
    for team in data:
        for p in team["players"]:
            p["display_name"] = get_display_name(p["player_name"])
        for p in team["top11"]:
            p["display_name"] = get_display_name(p["player_name"])
    return data


@app.get("/api/team/{team_name}")
async def team_detail(team_name: str):
    data = db.get_team_detail(team_name)
    if not data:
        raise HTTPException(404, "Team not found")
    for p in data["players"]:
        p["display_name"] = get_display_name(p["player_name"])
    return data


@app.get("/api/live")
async def live():
    """Get latest match data with per-team fantasy impact."""
    latest = db.get_latest_match()
    if not latest:
        return {"match": None, "team_impacts": []}

    match_points = db.get_live_match_points(latest["match_id"])
    standings_data = db.get_standings()

    player_to_team = {}
    for team in standings_data:
        for p in team["players"]:
            player_to_team[p["player_name"]] = team["team_name"]

    team_impacts: dict = {}
    for pp in match_points:
        team_name = player_to_team.get(pp["player_name"])
        if not team_name:
            continue
        if team_name not in team_impacts:
            team_impacts[team_name] = {"team_name": team_name, "match_pts": 0, "players": []}
        pp["display_name"] = get_display_name(pp["player_name"])
        team_impacts[team_name]["players"].append(pp)
        team_impacts[team_name]["match_pts"] += pp["total_pts"]

    impacts = sorted(team_impacts.values(), key=lambda x: x["match_pts"], reverse=True)
    for t in impacts:
        t["players"].sort(key=lambda x: x["total_pts"], reverse=True)

    return {
        "match": {
            "match_id": latest["match_id"],
            "date": latest["date"],
            "teams": latest["teams"],
            "venue": latest["venue"],
            "status": latest["status"],
        },
        "team_impacts": impacts,
    }


@app.get("/api/awards")
async def awards():
    """All awards and stats for the season."""
    data = db.get_awards()

    def _add_display_names(obj):
        if obj is None:
            return
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict) and "player_name" in item:
                    item["display_name"] = get_display_name(item["player_name"])
        elif isinstance(obj, dict) and "player_name" in obj:
            obj["display_name"] = get_display_name(obj["player_name"])

    for key in data:
        _add_display_names(data[key])

    return data


@app.get("/api/h2h")
async def head_to_head(team1: str, team2: str):
    """Head-to-head comparison between two teams."""
    data = db.get_head_to_head(team1, team2)
    if not data:
        raise HTTPException(404, "One or both teams not found")
    for side in ("team1", "team2"):
        for p in data[side]["players"]:
            p["display_name"] = get_display_name(p["player_name"])
    return data


# ------------------------------------------------------------------
# Admin endpoints
# ------------------------------------------------------------------

@app.post("/api/admin/roster")
async def update_roster(request: Request):
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Invalid secret")

    conn = db.get_db()
    team = conn.execute(
        "SELECT team_id FROM teams WHERE team_name = ?", (body["team_name"],)
    ).fetchone()
    if not team:
        conn.close()
        raise HTTPException(404, "Team not found")

    today = datetime.now().strftime("%Y-%m-%d")

    if body.get("remove_player"):
        conn.execute(
            "UPDATE roster SET removed_date = ? WHERE team_id = ? AND player_name = ? AND removed_date IS NULL",
            (today, team["team_id"], body["remove_player"]),
        )

    if body.get("add_player"):
        conn.execute(
            "INSERT INTO roster (team_id, player_name, added_date) VALUES (?, ?, ?)",
            (team["team_id"], body["add_player"], today),
        )

    conn.commit()
    conn.close()
    return {"status": "ok"}


@app.post("/api/admin/refresh")
async def force_refresh(request: Request):
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Invalid secret")
    poll_live_matches()
    return {"status": "refreshed"}


@app.post("/api/admin/reseed")
async def reseed(request: Request):
    """Wipe all match data and re-fetch from Cricbuzz + Cricsheet."""
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Invalid secret")
    db.wipe_match_data()
    fetch_and_store_completed_matches()
    rescore_from_cricsheet()
    return {"status": "reseeded", "matches": db.get_match_count()}


@app.post("/api/admin/rescore-cricsheet")
async def force_rescore(request: Request):
    """Force re-score all completed matches from Cricsheet data."""
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Invalid secret")
    rescore_from_cricsheet()
    return {"status": "rescored", "matches": db.get_match_count()}


@app.get("/api/status")
async def status():
    """Health check — shows DB state."""
    conn = db.get_db()
    team_count = conn.execute("SELECT COUNT(*) as cnt FROM teams").fetchone()["cnt"]
    match_count = conn.execute("SELECT COUNT(*) as cnt FROM matches").fetchone()["cnt"]
    player_pts_count = conn.execute(
        "SELECT COUNT(DISTINCT player_name) as cnt FROM player_match_points"
    ).fetchone()["cnt"]
    matches = conn.execute(
        "SELECT match_id, date, teams_json, status FROM matches ORDER BY date DESC"
    ).fetchall()
    conn.close()

    return {
        "teams": team_count,
        "matches_stored": match_count,
        "players_with_points": player_pts_count,
        "match_list": [
            {
                "match_id": m["match_id"],
                "date": m["date"],
                "teams": m["teams_json"],
                "status": m["status"],
            }
            for m in matches
        ],
        "data_source": "cricbuzz",
        "cricbuzz_key_set": bool(CRICBUZZ_API_KEY),
        "poller_running": scheduler.running,
    }
