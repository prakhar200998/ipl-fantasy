"""FastAPI app — routes, startup, background poller."""
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.background import BackgroundScheduler

import database as db
from adapters.cricsheet import CricsheetAdapter
from adapters.cricketdata import CricketDataAdapter
from scoring import calculate_fantasy_points
from name_mapping import get_cricsheet_name, get_display_name
from config import (
    CRICSHEET_DATA_DIR, CRICKETDATA_API_KEY, SEASON,
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


def poll_live_matches():
    """Background job: fetch live match data and update points."""
    if not CRICKETDATA_API_KEY:
        return
    try:
        adapter = CricketDataAdapter()
        matches = adapter.get_match_list(SEASON)
        for match in matches:
            if match["status"] != "in_progress":
                continue
            scorecard = adapter.get_scorecard(match["match_id"])
            if not scorecard:
                continue
            points = calculate_fantasy_points(scorecard)
            db.upsert_match(
                match["match_id"], match["date"], match["teams"],
                match["venue"], "in_progress"
            )
            db.bulk_upsert_player_points(match["match_id"], points)
            logger.info("Updated live match %s", match["match_id"])
    except Exception as e:
        logger.error("Poll error: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    # Seed if empty (only works if Cricsheet data dir exists locally)
    if db.get_match_count() == 0:
        import os
        if os.path.isdir(CRICSHEET_DATA_DIR):
            logger.info("Empty DB — seeding from Cricsheet files...")
            from scripts.seed_db import main as seed_main
            seed_main()
        else:
            logger.info("Empty DB and no Cricsheet data dir — skipping seed")

    if CRICKETDATA_API_KEY:
        interval = LIVE_POLL_INTERVAL if is_match_hours() else IDLE_POLL_INTERVAL
        scheduler.add_job(poll_live_matches, "interval", seconds=interval, id="poller")
        scheduler.start()
        logger.info("Scheduler started (interval=%ds)", interval)
    else:
        logger.info("No API key set — live polling disabled")

    yield

    if scheduler.running:
        scheduler.shutdown()


app = FastAPI(title="IPL Fantasy League", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/standings")
async def standings():
    data = db.get_standings()
    # Add display names
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
    standings = db.get_standings()

    # Map players to their fantasy teams
    player_to_team = {}
    for team in standings:
        for p in team["players"]:
            player_to_team[p["player_name"]] = team["team_name"]

    # Build per-team impact for this match
    team_impacts = {}
    for pp in match_points:
        team_name = player_to_team.get(pp["player_name"])
        if not team_name:
            continue
        if team_name not in team_impacts:
            team_impacts[team_name] = {"team_name": team_name, "match_pts": 0, "players": []}
        pp["display_name"] = get_display_name(pp["player_name"])
        team_impacts[team_name]["players"].append(pp)
        team_impacts[team_name]["match_pts"] += pp["total_pts"]

    # Sort teams by match impact, players within teams by points
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

    # Remove player
    if body.get("remove_player"):
        cs_name = get_cricsheet_name(body["remove_player"])
        conn.execute(
            "UPDATE roster SET removed_date = ? WHERE team_id = ? AND player_name = ? AND removed_date IS NULL",
            (today, team["team_id"], cs_name)
        )

    # Add player
    if body.get("add_player"):
        cs_name = get_cricsheet_name(body["add_player"])
        conn.execute(
            "INSERT INTO roster (team_id, player_name, added_date) VALUES (?, ?, ?)",
            (team["team_id"], cs_name, today)
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
