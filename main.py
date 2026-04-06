"""FastAPI app — routes, startup, background poller.

Data-source strategy:
  Discovery:    CricketData.org series_info (1 credit, returns ALL matches)
  Scorecards:   ESPN free API (primary — correct economy/dots, unlimited)
  Live scores:  ESPN free API (every 1 min during match hours)
  Enrichment:   CricketData.org match_scorecard (10 credits, adds lbw/bowled/runouts)
  Re-scoring:   Cricsheet ball-by-ball (admin-triggered only)
  Dormant:      Cricbuzz API via RapidAPI (available via admin)

Startup: restore backup → ESPN re-score (free) → CD discovery (1 credit) → backup
Deploy cost: 1 credit (series_info only). Zero match_scorecard calls.

Daily budget (90 cap):
  Discovery:  6 series_info crons = 6 credits
  CD refresh: 7 weekday / 8 weekend crons × 10 = 70-80 credits (0 if no live match)
  Total:      ≤ 86 credits/day
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
from adapters.espn import enrich_bowling_dots, get_espn_scorecard, discover_espn_matches
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

def _get_stored_match_map() -> dict[tuple[str, tuple], str]:
    """Return {(date, sorted_teams): match_id} for all stored complete/abandoned matches."""
    conn = db.get_db()
    rows = conn.execute(
        "SELECT match_id, date, teams_json FROM matches WHERE status IN ('complete', 'abandoned')"
    ).fetchall()
    conn.close()
    result = {}
    for row in rows:
        teams = tuple(sorted(json.loads(row["teams_json"]))) if row["teams_json"] else ()
        result[(row["date"], teams)] = row["match_id"]
    return result


def fetch_and_store_matches():
    """Discover IPL 2026 matches and score NEW ones via ESPN.

    Discovery: CricketData series_info (1 credit) → fallback to ESPN (free, scans dates).
    Scoring: ESPN (free, correct economy/dots). Zero match_scorecard calls.
    Already-stored matches are skipped — no re-fetching on redeploy.
    """
    actionable = []

    # Try CricketData first (1 credit, returns ALL matches at once)
    if CRICKETDATA_API_KEY:
        cd = CricketDataAdapter()
        all_matches = cd.get_match_list(SEASON)
        actionable = [m for m in all_matches if m["status"] in ("complete", "abandoned", "in_progress")]
        if actionable:
            logger.info("CricketData: found %d actionable matches", len(actionable))

    # Fallback: ESPN discovery (free, scans date range)
    if not actionable:
        logger.info("CricketData unavailable — falling back to ESPN discovery")
        today = datetime.now(IST).strftime("%Y-%m-%d")
        espn_matches = discover_espn_matches("2026-03-28", today)
        for em in espn_matches:
            if em["status"] in ("complete", "abandoned", "in_progress"):
                actionable.append({
                    "match_id": f"espn_{em['espn_id']}",
                    "date": em["date"],
                    "teams": em["teams"],
                    "venue": "",
                    "status": em["status"],
                    "name": em["name"],
                })
        if actionable:
            logger.info("ESPN discovery: found %d actionable matches", len(actionable))

    if not actionable:
        return

    # Map (date, sorted_teams) → existing match_id for cross-source dedup
    stored_map = _get_stored_match_map()

    conn = db.get_db()
    stored_statuses = {
        row["match_id"]: row["status"]
        for row in conn.execute("SELECT match_id, status FROM matches").fetchall()
    }
    conn.close()

    captain_vc = get_captain_vc()
    stored_count = 0

    for match in actionable:
        stored_status = stored_statuses.get(match["match_id"])
        key = (match["date"][:10], tuple(sorted(match["teams"])))
        existing_id = stored_map.get(key)
        match_id = existing_id or match["match_id"]

        # Skip already-finalized matches (by ID or by dedup)
        if stored_status in ("complete", "abandoned"):
            continue
        if existing_id:
            continue

        # in_progress already tracked — ESPN refresh handles live updates
        if stored_status == "in_progress" and match["status"] == "in_progress":
            continue

        if match["status"] == "abandoned":
            db.upsert_match(
                match_id, match["date"], match["teams"],
                match["venue"], "abandoned",
            )
            db.insert_washout_zeroes(match_id, match["teams"], captain_vc)
            logger.info("Stored abandoned match %s: %s", match_id, match.get("name", ""))
            stored_map[key] = match_id
            stored_count += 1
            continue

        # Score from ESPN (free, correct economy/dots)
        scorecard = get_espn_scorecard(match["date"][:10], match["teams"])
        if not scorecard:
            if match["status"] == "in_progress":
                db.upsert_match(
                    match_id, match["date"], match["teams"],
                    match["venue"], "in_progress",
                )
                stored_count += 1
            else:
                logger.warning("ESPN failed for %s — skipping", match.get("name", match_id))
            continue
        scorecard.match_id = match_id

        points = calculate_fantasy_points(scorecard)
        db.upsert_match(
            match_id, match["date"], match["teams"],
            match["venue"], match["status"],
        )
        db.bulk_upsert_player_points(match_id, points, captain_vc, force=True)
        stored_map[key] = match_id
        stored_count += 1
        logger.info("Stored match %s: %s (status=%s)", match_id, match.get("name", ""), match["status"])

        if match["status"] == "complete":
            db.backup_to_remote()

    if stored_count == 0:
        logger.info("All matches already stored")


def _get_existing_enrichment(match_id: str) -> dict:
    """Read lbw_bowled, direct_runouts, and assisted_runouts from existing breakdown_json.

    Returns {"lbw_bowled": {player: count}, "direct_runouts": {player: count},
             "assisted_runouts": {player: count}}.
    Backward compat: old "runouts" key maps to direct_runouts.
    """
    conn = db.get_db()
    rows = conn.execute(
        "SELECT player_name, breakdown_json FROM player_match_points WHERE match_id = ?",
        (match_id,),
    ).fetchall()
    conn.close()

    lbw_bowled: dict[str, int] = {}
    direct_runouts: dict[str, int] = {}
    assisted_runouts: dict[str, int] = {}
    for row in rows:
        if not row["breakdown_json"]:
            continue
        bd = json.loads(row["breakdown_json"])
        bowl = bd.get("bowling", {})
        if bowl.get("lbw_bowled", 0) > 0:
            lbw_bowled[row["player_name"]] = bowl["lbw_bowled"]
        fld = bd.get("fielding", {})
        if fld.get("direct_runouts", 0) > 0:
            direct_runouts[row["player_name"]] = fld["direct_runouts"]
        if fld.get("assisted_runouts", 0) > 0:
            assisted_runouts[row["player_name"]] = fld["assisted_runouts"]
        # Backward compat: old "runouts" key (pre-migration) → direct_runouts
        if "runouts" in fld and "direct_runouts" not in fld:
            if fld["runouts"] > 0:
                direct_runouts[row["player_name"]] = fld["runouts"]
    return {"lbw_bowled": lbw_bowled, "direct_runouts": direct_runouts,
            "assisted_runouts": assisted_runouts}


def _inject_enrichment(scorecard: "MatchScorecard", enrichment: dict) -> None:
    """Inject lbw_bowled, direct_runouts, and assisted_runouts into a scorecard."""
    from models import FieldingEntry

    for player, count in enrichment["lbw_bowled"].items():
        if player in scorecard.bowling:
            scorecard.bowling[player].lbw_bowled = count

    for player, count in enrichment.get("direct_runouts", {}).items():
        if player in scorecard.fielding:
            scorecard.fielding[player].direct_runouts = count
        else:
            scorecard.fielding[player] = FieldingEntry(player=player, direct_runouts=count)

    for player, count in enrichment.get("assisted_runouts", {}).items():
        if player in scorecard.fielding:
            scorecard.fielding[player].assisted_runouts = count
        else:
            scorecard.fielding[player] = FieldingEntry(player=player, assisted_runouts=count)


def _rescore_existing_espn():
    """Re-score all complete matches from ESPN (free, 0 credits).

    Fixes economy/dots for matches restored from backup.
    Runs once on startup.
    """
    conn = db.get_db()
    rows = conn.execute(
        "SELECT match_id, date, teams_json FROM matches WHERE status = 'complete'"
    ).fetchall()
    conn.close()

    if not rows:
        return

    captain_vc = get_captain_vc()
    rescored = 0

    for row in rows:
        teams = json.loads(row["teams_json"]) if row["teams_json"] else []
        date = row["date"][:10] if row["date"] else ""
        scorecard = get_espn_scorecard(date, teams)
        if not scorecard:
            continue
        scorecard.match_id = row["match_id"]
        _inject_enrichment(scorecard, _get_existing_enrichment(row["match_id"]))
        points = calculate_fantasy_points(scorecard)
        db.bulk_upsert_player_points(row["match_id"], points, captain_vc, force=True)
        rescored += 1

    if rescored:
        logger.info("ESPN re-scored %d existing matches (free)", rescored)


def discover_matches():
    """Cron-scheduled around known IPL start times (3:30 PM, 7:30 PM IST).

    Runs at: 3:15, 3:45, 7:15, 7:45, 9:00, 10:30 IST
    Costs 1 API call (series_info) + 1 per new match found (match_scorecard).
    ~6 calls/day — leaves budget for 5-min live refresh.
    """
    if not CRICKETDATA_API_KEY:
        return

    try:
        fetch_and_store_matches()
    except Exception as e:
        logger.error("Discovery error: %s", e)


def refresh_live_espn():
    """Every 1 min: free ESPN scorecard refresh for live matches.

    ESPN provides: batting, bowling (with dots), catches, stumpings, playing XI.
    Missing: lbw/bowled bonus, runout credits (added by CD cron refresh).
    Gated: weekdays 7:30-11:30 PM IST, weekends 3:30-11:30 PM IST.
    """
    now_ist = datetime.now(IST)
    dow = now_ist.weekday()  # 0=Mon, 5=Sat, 6=Sun
    is_weekend = dow >= 5
    start = 15 * 60 + 30 if is_weekend else 19 * 60 + 30  # 3:30 PM or 7:30 PM
    end = 23 * 60 + 30  # 11:30 PM
    now_mins = now_ist.hour * 60 + now_ist.minute
    if not (start <= now_mins < end):
        return

    conn = db.get_db()
    rows = conn.execute(
        "SELECT match_id, date, teams_json FROM matches WHERE status = 'in_progress'"
    ).fetchall()
    conn.close()

    if not rows:
        return

    captain_vc = get_captain_vc()

    for row in rows:
        try:
            teams = json.loads(row["teams_json"]) if row["teams_json"] else []
            date = row["date"][:10] if row["date"] else ""
            scorecard = get_espn_scorecard(date, teams)
            if not scorecard:
                continue
            scorecard.match_id = row["match_id"]
            _inject_enrichment(scorecard, _get_existing_enrichment(row["match_id"]))
            points = calculate_fantasy_points(scorecard)
            # force=True: live scores can go up/down (SR penalties etc.)
            db.bulk_upsert_player_points(row["match_id"], points, captain_vc, force=True)

            if scorecard.status == "complete":
                # ESPN sees match done → final CricketData fetch for lbw/bowled/runouts
                _finalize_match_cd(row["match_id"], row["date"], teams, captain_vc)
            else:
                logger.info("ESPN refreshed live match %s", row["match_id"])
        except Exception as e:
            logger.error("ESPN refresh error for %s: %s", row["match_id"], e)


def _finalize_match_cd(match_id: str, date: str, teams: list, captain_vc: dict):
    """Final CricketData fetch when match completes — gets lbw/bowled/runouts."""
    if not CRICKETDATA_API_KEY:
        db.upsert_match(match_id, date, teams, "", "complete")
        db.backup_to_remote()
        return
    try:
        cd = CricketDataAdapter()
        scorecard = cd.get_scorecard(match_id)
        if scorecard:
            enrich_bowling_dots(scorecard, date[:10] if date else "", teams)
            points = calculate_fantasy_points(scorecard)
            db.upsert_match(match_id, date, teams, scorecard.venue or "", "complete")
            db.bulk_upsert_player_points(match_id, points, captain_vc, force=True)
            db.set_enrichment_version(match_id, "cd_v2")
        else:
            db.upsert_match(match_id, date, teams, "", "complete")
        db.backup_to_remote()
        logger.info("Match %s finalized with CricketData", match_id)
    except Exception as e:
        logger.error("Finalize error for %s: %s", match_id, e)
        db.upsert_match(match_id, date, teams, "", "complete")
        db.backup_to_remote()


def refresh_live_cd():
    """Cron-scheduled CricketData scorecard for live matches.

    Adds lbw/bowled bonus and runout credits that ESPN doesn't have.
    Weekdays: 7 calls (7:30-11:30 PM IST), Weekends: 8 calls (3:30-11:30 PM IST).
    No-op if no live matches → 0 credits.
    """
    if not CRICKETDATA_API_KEY:
        return

    conn = db.get_db()
    rows = conn.execute(
        "SELECT match_id, date, teams_json FROM matches WHERE status = 'in_progress'"
    ).fetchall()
    conn.close()

    if not rows:
        return

    try:
        cd = CricketDataAdapter()
        captain_vc = get_captain_vc()

        for row in rows:
            scorecard = cd.get_scorecard(row["match_id"])
            if not scorecard:
                continue
            teams = json.loads(row["teams_json"]) if row["teams_json"] else []
            enrich_bowling_dots(scorecard, row["date"][:10] if row["date"] else "", teams)
            points = calculate_fantasy_points(scorecard)
            db.upsert_match(
                row["match_id"], row["date"], teams,
                scorecard.venue or "", scorecard.status,
            )
            db.bulk_upsert_player_points(row["match_id"], points, captain_vc, force=True)

            if scorecard.status == "complete":
                db.set_enrichment_version(row["match_id"], "cd_v2")
                db.backup_to_remote()
                logger.info("Match %s completed (detected by CD refresh)", row["match_id"])
            else:
                logger.info("CD refreshed live match %s", row["match_id"])

    except Exception as e:
        logger.error("CD live refresh error: %s", e)


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


def _needs_cricsheet_backfill() -> bool:
    """Check if any bowler has wickets but no lbw_bowled key, or old runouts format."""
    conn = db.get_db()
    rows = conn.execute(
        "SELECT breakdown_json FROM player_match_points WHERE bowling_pts > 0 OR fielding_pts > 0 LIMIT 100"
    ).fetchall()
    conn.close()
    for row in rows:
        if not row["breakdown_json"]:
            continue
        bd = json.loads(row["breakdown_json"])
        bowl = bd.get("bowling", {})
        if bowl.get("wickets", 0) > 0 and "lbw_bowled" not in bowl:
            return True
        fld = bd.get("fielding", {})
        if "runouts" in fld and "direct_runouts" not in fld:
            return True
    return False


def _enrich_from_cricketdata():
    """Re-fetch CricketData for completed matches missing enrichment.

    Uses enrichment_version column to detect unenriched matches.
    Fetches CricketData scorecard, enriches with lbw_bowled + direct/assisted runouts + ESPN dots,
    re-scores and saves. Gated behind CRICKETDATA_API_KEY and respects daily limit.
    """
    if not CRICKETDATA_API_KEY:
        return

    conn = db.get_db()
    to_enrich = conn.execute(
        "SELECT match_id, date, teams_json FROM matches "
        "WHERE status = 'complete' AND (enrichment_version IS NULL OR enrichment_version != 'cd_v2')"
    ).fetchall()
    conn.close()

    if not to_enrich:
        logger.info("CricketData enrichment: all matches already enriched")
        return

    logger.info("CricketData enrichment: %d matches need runout enrichment", len(to_enrich))

    cd = CricketDataAdapter()
    captain_vc = get_captain_vc()
    enriched = 0

    for row in to_enrich:
        # Check daily limit before each call (get_scorecard costs 10 credits)
        from adapters.cricketdata import _daily_call_log, CRICKETDATA_DAILY_LIMIT
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if _daily_call_log["date"] == today and _daily_call_log["calls"] >= CRICKETDATA_DAILY_LIMIT:
            logger.warning("CricketData enrichment: daily limit reached, %d/%d enriched",
                           enriched, len(to_enrich))
            break

        cd_scorecard = cd.get_scorecard(row["match_id"])
        if not cd_scorecard:
            continue

        # Enrich with ESPN dots (CricketData doesn't have dot balls)
        teams = json.loads(row["teams_json"]) if row["teams_json"] else []
        enrich_bowling_dots(cd_scorecard, row["date"][:10] if row["date"] else "", teams)

        points = calculate_fantasy_points(cd_scorecard)
        db.bulk_upsert_player_points(row["match_id"], points, captain_vc, force=True)
        db.set_enrichment_version(row["match_id"], "cd_v2")
        enriched += 1
        logger.info("CricketData enriched match %s (direct/assisted runouts + lbw_bowled)", row["match_id"])

    if enriched:
        logger.info("CricketData enrichment complete: %d matches updated", enriched)


def _backfill_from_cricsheet():
    """One-time Cricsheet backfill for lbw_bowled and runouts.

    Detects if backfill is needed (bowlers with wickets but missing lbw_bowled key).
    Downloads Cricsheet ZIP (~5MB), extracts lbw_bowled/runouts, combines with
    ESPN scorecard (for dots), re-scores. Skips instantly on subsequent deploys.
    """
    if not _needs_cricsheet_backfill():
        logger.info("Cricsheet backfill: not needed (lbw_bowled key already present)")
        return

    logger.info("Cricsheet backfill: lbw_bowled missing — downloading Cricsheet data")

    if not download_cricsheet_ipl(CRICSHEET_DATA_DIR):
        logger.error("Cricsheet backfill: download failed")
        return

    cs_adapter = CricsheetAdapter(CRICSHEET_DATA_DIR)
    captain_vc = get_captain_vc()

    conn = db.get_db()
    db_matches = conn.execute(
        "SELECT match_id, date, teams_json FROM matches WHERE status = 'complete'"
    ).fetchall()
    conn.close()

    if not db_matches:
        return

    backfilled = 0
    for row in db_matches:
        teams = json.loads(row["teams_json"]) if row["teams_json"] else []
        date = row["date"] or ""

        cs_match_id = find_cricsheet_match_id(cs_adapter, SEASON, date[:10], teams)
        if not cs_match_id:
            continue

        cs_scorecard = cs_adapter.get_scorecard(cs_match_id)
        if not cs_scorecard:
            continue

        # Extract lbw_bowled and runouts from Cricsheet
        enrichment: dict = {"lbw_bowled": {}, "direct_runouts": {}, "assisted_runouts": {}}
        for player, bw in cs_scorecard.bowling.items():
            if bw.lbw_bowled > 0:
                enrichment["lbw_bowled"][player] = bw.lbw_bowled
        for player, fl in cs_scorecard.fielding.items():
            if fl.direct_runouts > 0:
                enrichment["direct_runouts"][player] = fl.direct_runouts
            if fl.assisted_runouts > 0:
                enrichment["assisted_runouts"][player] = fl.assisted_runouts

        # Build ESPN scorecard (for dots/economy) and inject Cricsheet enrichment
        espn_scorecard = get_espn_scorecard(date[:10], teams)
        if not espn_scorecard:
            continue
        espn_scorecard.match_id = row["match_id"]
        _inject_enrichment(espn_scorecard, enrichment)

        points = calculate_fantasy_points(espn_scorecard)
        db.bulk_upsert_player_points(row["match_id"], points, captain_vc, force=True)
        backfilled += 1

    logger.info("Cricsheet backfill complete: %d matches updated", backfilled)


def _deferred_startup():
    """Runs once after app starts serving — avoids blocking Render health check."""
    try:
        _rescore_existing_espn()
    except Exception as e:
        logger.error("ESPN re-score error: %s", e)

    try:
        fetch_and_store_matches()
    except Exception as e:
        logger.error("Startup fetch error (will retry via poller): %s", e)

    try:
        _enrich_from_cricketdata()
    except Exception as e:
        logger.error("CricketData enrichment error: %s", e)

    try:
        _backfill_from_cricsheet()
    except Exception as e:
        logger.error("Cricsheet backfill error: %s", e)

    db.backup_to_remote()
    logger.info("Deferred startup complete: %d matches in DB", db.get_match_count())


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

    logger.info("DB has %d matches after restore", db.get_match_count())

    # Start background scheduler
    from apscheduler.triggers.cron import CronTrigger

    if CRICKETDATA_API_KEY:
        # Discovery: cron at known IPL start windows (~6 series_info calls/day = 6 credits)
        discovery_times_ist = [(15, 15), (15, 45), (19, 15), (19, 45), (21, 0), (22, 30)]
        for hour, minute in discovery_times_ist:
            utc_total = hour * 60 + minute - 330  # IST offset = 5h30m
            utc_h, utc_m = utc_total // 60, utc_total % 60
            scheduler.add_job(
                discover_matches,
                CronTrigger(hour=utc_h, minute=utc_m),
                id=f"discover_{hour:02d}{minute:02d}",
            )
            logger.info("Discovery at %02d:%02d IST (UTC %02d:%02d)", hour, minute, utc_h, utc_m)

        # CricketData scorecard refresh (10 credits each, adds lbw/bowled/runouts)
        # Weekdays: 7 calls between 7:30-11:30 PM IST (~every 34 min)
        weekday_cd_ist = [(19, 30), (20, 5), (20, 40), (21, 15), (21, 50), (22, 25), (23, 0)]
        for hour, minute in weekday_cd_ist:
            utc_total = hour * 60 + minute - 330
            utc_h, utc_m = utc_total // 60, utc_total % 60
            scheduler.add_job(
                refresh_live_cd,
                CronTrigger(day_of_week="mon-fri", hour=utc_h, minute=utc_m),
                id=f"cd_wd_{hour:02d}{minute:02d}",
            )
            logger.info("CD refresh weekday %02d:%02d IST (UTC %02d:%02d)", hour, minute, utc_h, utc_m)

        # Weekends: 8 calls between 3:30-11:30 PM IST (~every 60 min)
        weekend_cd_ist = [(15, 45), (17, 0), (18, 0), (19, 30), (20, 30), (21, 30), (22, 30), (23, 15)]
        for hour, minute in weekend_cd_ist:
            utc_total = hour * 60 + minute - 330
            utc_h, utc_m = utc_total // 60, utc_total % 60
            scheduler.add_job(
                refresh_live_cd,
                CronTrigger(day_of_week="sat,sun", hour=utc_h, minute=utc_m),
                id=f"cd_we_{hour:02d}{minute:02d}",
            )
            logger.info("CD refresh weekend %02d:%02d IST (UTC %02d:%02d)", hour, minute, utc_h, utc_m)

    # ESPN live refresh: every 1 min, FREE (time-gated inside the function)
    scheduler.add_job(
        refresh_live_espn, "interval",
        minutes=1, id="espn_live",
    )

    # Keep-alive self-ping (prevents Render free tier sleep during match hours)
    scheduler.add_job(
        _keep_alive_ping, "interval",
        minutes=10, id="keep_alive",
    )

    # Deferred startup: run after app is serving (avoids Render health check timeout)
    scheduler.add_job(
        _deferred_startup, "date",
        id="deferred_startup",
    )

    scheduler.start()
    if CRICKETDATA_API_KEY:
        logger.info("Scheduler started: 6 discovery, 7 weekday CD, 8 weekend CD, ESPN 1m, keep-alive 10m")
    else:
        logger.info("Scheduler started: ESPN 1m + keep-alive (no CD key — discovery/CD refresh disabled)")

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
    fetch_and_store_matches()
    refresh_live_espn()
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
    from adapters.cricketdata import get_daily_usage
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
        "cricketdata_daily_usage": get_daily_usage(),
        "cricbuzz_monthly_usage": get_api_usage(),
    }
