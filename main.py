"""FastAPI app — routes, startup, background poller.

Data-source strategy:
  Primary (live + completed): CricketData.org API (series_info returns ALL matches)
  Dots enrichment:            ESPN free API (accurate per-bowler dot balls)
  Accuracy pass (manual):     Cricsheet ball-by-ball (admin-triggered only)
  Dormant fallback:           Cricbuzz API via RapidAPI (available via admin)
"""
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.background import BackgroundScheduler

import database as db
from adapters.cricketdata import CricketDataAdapter
from adapters.espn import enrich_bowling_dots
from adapters.cricsheet import (
    CricsheetAdapter, download_cricsheet_ipl, find_cricsheet_match_id,
)
from scoring import calculate_fantasy_points
from name_mapping import get_display_name
from teams import get_captain_vc
from config import (
    CRICBUZZ_API_KEY, CRICKETDATA_API_KEY, CRICSHEET_DATA_DIR, SEASON,
    ADMIN_SECRET,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
scheduler = BackgroundScheduler()


def _keep_alive_ping():
    """Self-ping to prevent Render free tier from sleeping during match hours."""
    now_ist = datetime.now(IST)
    if not (14 <= now_ist.hour < 24):
        return
    try:
        import httpx
        httpx.get("https://ipl-fantasy-5soj.onrender.com/api/status", timeout=10)
    except Exception:
        pass


# ------------------------------------------------------------------
# Data fetching — CricketData.org primary
# ------------------------------------------------------------------

def _get_stored_match_keys() -> set[tuple[str, tuple]]:
    """Return set of (date, sorted_teams) for all stored complete/abandoned matches."""
    conn = db.get_db()
    rows = conn.execute(
        "SELECT date, teams_json FROM matches WHERE status IN ('complete', 'abandoned')"
    ).fetchall()
    conn.close()
    keys = set()
    for row in rows:
        teams = tuple(sorted(json.loads(row["teams_json"]))) if row["teams_json"] else ()
        keys.add((row["date"], teams))
    return keys


def fetch_and_store_matches():
    """Fetch ALL completed/abandoned IPL 2026 matches from CricketData.org and store points.

    Uses series_info which returns every match in the series (not just recent),
    so it recovers matches that would otherwise be lost across Render redeploys.
    """
    if not CRICKETDATA_API_KEY:
        logger.warning("No CRICKETDATA_API_KEY set — skipping fetch")
        return

    cd = CricketDataAdapter()
    all_matches = cd.get_match_list(SEASON)
    completed = [m for m in all_matches if m["status"] in ("complete", "abandoned")]
    logger.info("CricketData: found %d completed/abandoned IPL 2026 matches", len(completed))

    if not completed:
        return

    # Dedup by date + sorted teams (handles existing Cricbuzz-ID matches in DB)
    stored_keys = _get_stored_match_keys()

    captain_vc = get_captain_vc()
    stored_count = 0

    for match in completed:
        key = (match["date"][:10], tuple(sorted(match["teams"])))
        if key in stored_keys:
            continue

        if match["status"] == "abandoned":
            db.upsert_match(
                match["match_id"], match["date"], match["teams"],
                match["venue"], "abandoned",
            )
            db.insert_washout_zeroes(match["match_id"], match["teams"], captain_vc)
            logger.info("Stored abandoned match %s: %s", match["match_id"], match.get("name", ""))
            stored_keys.add(key)
            stored_count += 1
            continue

        scorecard = cd.get_scorecard(match["match_id"])
        if not scorecard:
            logger.warning("No scorecard for match %s", match.get("name", match["match_id"]))
            continue
        enrich_bowling_dots(scorecard, match["date"][:10], match["teams"])
        points = calculate_fantasy_points(scorecard)
        db.upsert_match(
            match["match_id"], match["date"], match["teams"],
            match["venue"], "complete",
        )
        db.bulk_upsert_player_points(match["match_id"], points, captain_vc)
        stored_keys.add(key)
        stored_count += 1
        logger.info("Stored match %s: %s (with ESPN dots)", match["match_id"], match.get("name", ""))

    if stored_count == 0:
        logger.info("All completed matches already stored")


def poll_live_matches():
    """Background job: discover in-progress + newly completed matches via CricketData.org."""
    now_ist = datetime.now(IST)
    if not (14 <= now_ist.hour < 24):
        return

    if not CRICKETDATA_API_KEY:
        return

    try:
        cd = CricketDataAdapter()
        all_matches = cd.get_match_list(SEASON)
        active = [m for m in all_matches if m["status"] in ("in_progress", "complete", "abandoned")]

        if not active:
            return

        # Skip matches already stored as complete or abandoned
        conn = db.get_db()
        stored = {
            row["match_id"]: row["status"]
            for row in conn.execute("SELECT match_id, status FROM matches").fetchall()
        }
        conn.close()

        # Also build dedup keys for cross-source matching
        stored_keys = _get_stored_match_keys()

        captain_vc = get_captain_vc()

        for match in active:
            # Skip by direct ID match
            stored_status = stored.get(match["match_id"])
            if stored_status in ("complete", "abandoned"):
                continue

            # Skip by date+teams dedup (handles Cricbuzz-ID matches)
            key = (match["date"][:10], tuple(sorted(match["teams"])))
            if key in stored_keys:
                continue

            if match["status"] == "abandoned":
                db.upsert_match(
                    match["match_id"], match["date"], match["teams"],
                    match["venue"], "abandoned",
                )
                db.insert_washout_zeroes(match["match_id"], match["teams"], captain_vc)
                logger.info("Stored abandoned match %s", match["match_id"])
                db.backup_to_remote()
                continue

            scorecard = cd.get_scorecard(match["match_id"])
            if not scorecard:
                continue
            enrich_bowling_dots(scorecard, match["date"][:10], match["teams"])
            points = calculate_fantasy_points(scorecard)
            db.upsert_match(
                match["match_id"], match["date"], match["teams"],
                match["venue"], match["status"],
            )
            db.bulk_upsert_player_points(match["match_id"], points, captain_vc)
            logger.info(
                "Updated match %s (status=%s, source=cricketdata)",
                match["match_id"], match["status"],
            )
            if match["status"] == "complete":
                db.backup_to_remote()

    except Exception as e:
        logger.error("Poll error: %s", e)


# ------------------------------------------------------------------
# Cricsheet re-scoring — admin-triggered only
# ------------------------------------------------------------------

def rescore_from_cricsheet():
    """Download Cricsheet ball-by-ball data and re-score completed matches.

    Uses force=True to bypass monotonicity guard since Cricsheet is
    the most accurate source (has real dot-ball counts).
    """
    try:
        if not download_cricsheet_ipl(CRICSHEET_DATA_DIR):
            return

        cs_adapter = CricsheetAdapter(CRICSHEET_DATA_DIR)
        captain_vc = get_captain_vc()

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
            db.bulk_upsert_player_points(row["match_id"], points, captain_vc, force=True)
            rescored += 1
            logger.info("Re-scored match %s from Cricsheet (with dots)", row["match_id"])

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

    from teams import TEAMS
    if team_count == 0:
        logger.info("No teams in DB — seeding teams...")
        db.seed_teams(TEAMS)
    else:
        db.reseed_rosters(TEAMS)

    # Restore persisted data FIRST so we never lose matches across deploys
    if db.get_match_count() == 0:
        logger.info("No matches in DB — restoring from remote backup")
        if not db.restore_from_remote():
            logger.info("Remote restore failed — loading seed file as last resort")
            db.load_seed_data()
        logger.info("After restore: %d matches in DB", db.get_match_count())

    # CricketData primary — discovers ALL matches (not just recent)
    try:
        fetch_and_store_matches()
    except Exception as e:
        logger.error("Startup fetch error (will retry via poller): %s", e)

    db.backup_to_remote()
    logger.info("DB has %d matches", db.get_match_count())

    # Start background scheduler
    if CRICKETDATA_API_KEY:
        # Single interval job: poll every 15 min during match hours
        scheduler.add_job(
            poll_live_matches, "interval",
            minutes=15, id="live_poll",
        )

        # Keep-alive self-ping (prevents Render free tier sleep during match hours)
        scheduler.add_job(
            _keep_alive_ping, "interval",
            minutes=10, id="keep_alive",
        )
        scheduler.start()
        logger.info("Scheduler started: poll every 15m, keep-alive 10m")
    else:
        logger.info("No CRICKETDATA_API_KEY set — scheduled fetching disabled")

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
    """Wipe all match data and re-fetch from CricketData.org.

    Safety: only wipes if the fresh fetch succeeds. Falls back to
    GitHub backup if API returns nothing.
    """
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Invalid secret")
    old_count = db.get_match_count()
    db.wipe_match_data()
    fetch_and_store_matches()
    new_count = db.get_match_count()
    if new_count == 0 and old_count > 0:
        logger.warning("Reseed fetch returned 0 matches — restoring from backup")
        db.restore_from_remote()
        new_count = db.get_match_count()
    return {"status": "reseeded", "matches": new_count}


@app.post("/api/admin/reseed-rosters")
async def reseed_rosters(request: Request):
    """Re-sync roster table from TEAMS dict (retroactive)."""
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Invalid secret")
    from teams import TEAMS
    db.reseed_rosters(TEAMS)
    return {"status": "rosters reseeded"}


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
    """Health check — shows DB state and API usage."""
    from adapters.cricbuzz import get_api_usage

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
        "data_source": "cricketdata",
        "cricketdata_key_set": bool(CRICKETDATA_API_KEY),
        "cricbuzz_key_set": bool(CRICBUZZ_API_KEY),
        "poller_running": scheduler.running,
        "cricbuzz_api_usage": get_api_usage(),
    }
