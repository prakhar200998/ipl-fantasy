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
    ADMIN_SECRET, PHASE2_CUTOFF_DATE,
)


# Rename map: old team_name in DB -> new Phase 2 team_name (matches keys in TEAMS_PHASE2)
TEAM_RENAME_MAP = {
    "Dark horse 11": "Dark Horse 11",
    "Rihen's Team": "Rihen",
    "Prakhar's Team": "Prakhar's Team",
    "Ee Sala Cup Namde FC": "ESALACUPNAMDE",
    "Shvetank's Team": "Shvetank's 11",
    "Ishan Jindal's Team": "Ary-ish 11",
    "Amal's Team": "Amal's Team",
    "Prasheel super 11": "Prasheel's Team",
    "Dhinchak Dudes": "Dhinchak Dudes",
}

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

    stored_count = 0

    for match in actionable:
        stored_status = stored_statuses.get(match["match_id"])
        key = (match["date"][:10], tuple(sorted(match["teams"])))
        existing_id = stored_map.get(key)
        match_id = existing_id or match["match_id"]
        captain_vc = get_captain_vc(match["date"][:10])

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
    """Re-score Phase 2 complete matches from ESPN (free, 0 credits).

    Phase 1 matches (date < PHASE2_CUTOFF_DATE) are intentionally skipped —
    their stored total_pts is the frozen snapshot baseline and must not move.
    """
    conn = db.get_db()
    rows = conn.execute(
        "SELECT match_id, date, teams_json FROM matches "
        "WHERE status = 'complete' AND date >= ?",
        (PHASE2_CUTOFF_DATE,),
    ).fetchall()
    conn.close()

    if not rows:
        return

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
        captain_vc = get_captain_vc(date)
        db.bulk_upsert_player_points(row["match_id"], points, captain_vc, force=True)
        rescored += 1

    if rescored:
        logger.info("ESPN re-scored %d Phase 2 matches (free)", rescored)


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

    for row in rows:
        try:
            teams = json.loads(row["teams_json"]) if row["teams_json"] else []
            date = row["date"][:10] if row["date"] else ""
            captain_vc = get_captain_vc(date)
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

        for row in rows:
            scorecard = cd.get_scorecard(row["match_id"])
            if not scorecard:
                continue
            teams = json.loads(row["teams_json"]) if row["teams_json"] else []
            date = row["date"][:10] if row["date"] else ""
            captain_vc = get_captain_vc(date)
            enrich_bowling_dots(scorecard, date, teams)
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

        conn = db.get_db()
        db_matches = conn.execute(
            "SELECT match_id, date, teams_json FROM matches "
            "WHERE status = 'complete' AND date >= ?",
            (PHASE2_CUTOFF_DATE,),
        ).fetchall()
        conn.close()

        if not db_matches:
            logger.info("No Phase 2 matches to re-score from Cricsheet")
            return

        rescored = 0
        for row in db_matches:
            teams = json.loads(row["teams_json"]) if row["teams_json"] else []
            date = row["date"] or ""
            captain_vc = get_captain_vc(date[:10])

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


def _resolve_cricsheet_name(cs_name: str, pool: set[str]) -> str | None:
    """Resolve a Cricsheet initial-style name (e.g. 'JC Archer') to a display
    name from a given pool (e.g. 'Jofra Archer').

    Exact match wins. Otherwise match by last name + first-initial; only
    accepted if the pool has a single candidate. Returns None if unresolvable.
    """
    if cs_name in pool:
        return cs_name
    parts = cs_name.split()
    if len(parts) < 2:
        return None
    last = parts[-1]
    first_initial = parts[0][0].upper()
    candidates = [
        d for d in pool
        if d.split() and d.split()[-1] == last
        and d.split()[0][:1].upper() == first_initial
    ]
    if len(candidates) == 1:
        return candidates[0]
    return None


def _backfill_enrichment_from_cricsheet():
    """Surgical Cricsheet backfill — only updates LBW/bowled and runout fields.

    For each complete match with enrichment_version IS NULL:
      1. Find Cricsheet match by date+teams.
      2. Extract lbw_bowled (per bowler) + direct/assisted_runouts (per fielder).
      3. For each existing player_match_points row, adjust breakdown values and
         apply the DELTA to bowling_pts / fielding_pts / raw_pts / total_pts.
         All other scoring (batting, dots, economy, milestones, catches, stumpings)
         is left untouched.
      4. Mark match as enrichment_version='cricsheet'.

    Free — single ZIP download, no API credits. Safe to re-run.
    """
    conn = db.get_db()
    to_enrich = conn.execute(
        "SELECT match_id, date, teams_json FROM matches "
        "WHERE status = 'complete' AND date >= ? "
        "AND (enrichment_version IS NULL OR enrichment_version != 'cricsheet')",
        (PHASE2_CUTOFF_DATE,),
    ).fetchall()
    conn.close()

    if not to_enrich:
        logger.info("Cricsheet enrichment: all Phase 2 matches already cricsheet-enriched")
        return

    logger.info("Cricsheet enrichment: %d Phase 2 matches need LBW/runout backfill", len(to_enrich))

    if not download_cricsheet_ipl(CRICSHEET_DATA_DIR):
        logger.error("Cricsheet enrichment: download failed")
        return

    cs_adapter = CricsheetAdapter(CRICSHEET_DATA_DIR)

    for row in to_enrich:
        teams = json.loads(row["teams_json"]) if row["teams_json"] else []
        date = (row["date"] or "")[:10]
        captain_vc = get_captain_vc(date)
        cs_match_id = find_cricsheet_match_id(cs_adapter, SEASON, date, teams)
        if not cs_match_id:
            logger.warning("Cricsheet enrichment: no match for %s %s", date, teams)
            continue

        cs_scorecard = cs_adapter.get_scorecard(cs_match_id)
        if not cs_scorecard:
            continue

        # Build display-name pool for THIS match from the stored player_match_points
        conn = db.get_db()
        xi_display = {r["player_name"] for r in conn.execute(
            "SELECT player_name FROM player_match_points WHERE match_id = ?",
            (row["match_id"],),
        ).fetchall()}
        conn.close()

        def _norm(cs_dict):
            out = {}
            for cs_name, count in cs_dict.items():
                disp = _resolve_cricsheet_name(cs_name, xi_display)
                if disp is not None:
                    out[disp] = out.get(disp, 0) + count
            return out

        lbw_bowled = _norm({p: bw.lbw_bowled for p, bw in cs_scorecard.bowling.items()
                            if bw.lbw_bowled > 0})
        direct_ro = _norm({p: fl.direct_runouts for p, fl in cs_scorecard.fielding.items()
                           if fl.direct_runouts > 0})
        assisted_ro = _norm({p: fl.assisted_runouts for p, fl in cs_scorecard.fielding.items()
                             if fl.assisted_runouts > 0})

        _apply_enrichment_delta(
            row["match_id"], lbw_bowled, direct_ro, assisted_ro, captain_vc,
        )
        db.set_enrichment_version(row["match_id"], "cricsheet")
        logger.info(
            "Cricsheet enriched match %s (lbw=%d, direct=%d, assisted=%d)",
            row["match_id"],
            sum(lbw_bowled.values()), sum(direct_ro.values()), sum(assisted_ro.values()),
        )

    db.backup_to_remote()


def _apply_enrichment_delta(match_id: str,
                            lbw_bowled: dict, direct_ro: dict, assisted_ro: dict,
                            captain_vc: dict[str, str]):
    """Apply Cricsheet-authoritative LBW/runout values to every player in the match.

    Cricsheet is the source of truth: any player absent from its dicts gets 0
    for that field (corrects any CD miscredit). Batting, dots, economy,
    milestones, catches, stumpings are left untouched — only bowling.lbw_bowled,
    fielding.direct_runouts and fielding.assisted_runouts move, and
    bowling_pts / fielding_pts / raw_pts / total_pts are adjusted by the exact
    delta (with C/VC multiplier).
    """
    conn = db.get_db()
    rows = conn.execute(
        "SELECT player_name, bowling_pts, fielding_pts, raw_pts, breakdown_json "
        "FROM player_match_points WHERE match_id = ?",
        (match_id,),
    ).fetchall()

    # Cricsheet-credited players not in our XI (e.g., sub fielders) are implicitly
    # skipped — we only iterate players with existing rows.

    for row in rows:
        player = row["player_name"]
        bd = json.loads(row["breakdown_json"]) if row["breakdown_json"] else {"playing_xi": 4}

        old_lbw = bd.get("bowling", {}).get("lbw_bowled", 0)
        old_direct = bd.get("fielding", {}).get("direct_runouts", 0)
        old_assisted = bd.get("fielding", {}).get("assisted_runouts", 0)

        # Cricsheet = source of truth; absent → 0
        new_lbw = lbw_bowled.get(player, 0)
        new_direct = direct_ro.get(player, 0)
        new_assisted = assisted_ro.get(player, 0)

        # Deltas
        d_bowling = (new_lbw - old_lbw) * 8
        d_fielding = (new_direct - old_direct) * 10 + (new_assisted - old_assisted) * 5
        d_raw = d_bowling + d_fielding

        if d_raw == 0:
            continue

        # Update breakdown in place — write fields whenever value changes
        if new_lbw != old_lbw:
            bd.setdefault("bowling", {})["lbw_bowled"] = new_lbw
        if new_direct != old_direct or new_assisted != old_assisted:
            fld = bd.setdefault("fielding", {
                "catches": 0, "direct_runouts": 0, "assisted_runouts": 0, "stumpings": 0,
            })
            fld["direct_runouts"] = new_direct
            fld["assisted_runouts"] = new_assisted

        new_bowling_pts = row["bowling_pts"] + d_bowling
        new_fielding_pts = row["fielding_pts"] + d_fielding
        new_raw_pts = row["raw_pts"] + d_raw

        designation = captain_vc.get(player, "")
        if designation == "C":
            new_total = new_raw_pts * 2
        elif designation == "VC":
            new_total = int(new_raw_pts * 1.5)
        else:
            new_total = new_raw_pts

        conn.execute(
            "UPDATE player_match_points SET bowling_pts = ?, fielding_pts = ?, "
            "raw_pts = ?, total_pts = ?, breakdown_json = ? "
            "WHERE match_id = ? AND player_name = ?",
            (new_bowling_pts, new_fielding_pts, new_raw_pts, new_total,
             json.dumps(bd), match_id, player),
        )
    conn.commit()
    conn.close()


def _enrich_from_cricketdata():
    """Fallback CricketData enrichment for matches Cricsheet couldn't cover.

    Uses enrichment_version column to detect unenriched matches.
    Fetches CricketData scorecard, enriches with lbw_bowled + direct/assisted runouts + ESPN dots,
    re-scores and saves. Gated behind CRICKETDATA_API_KEY and respects daily limit.
    """
    if not CRICKETDATA_API_KEY:
        return

    conn = db.get_db()
    to_enrich = conn.execute(
        "SELECT match_id, date, teams_json FROM matches "
        "WHERE status = 'complete' AND date >= ? "
        "AND (enrichment_version IS NULL OR enrichment_version != 'cd_v2')",
        (PHASE2_CUTOFF_DATE,),
    ).fetchall()
    conn.close()

    if not to_enrich:
        logger.info("CricketData enrichment: all Phase 2 matches already enriched")
        return

    logger.info("CricketData enrichment: %d Phase 2 matches need runout enrichment", len(to_enrich))

    cd = CricketDataAdapter()
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
        date = row["date"][:10] if row["date"] else ""
        enrich_bowling_dots(cd_scorecard, date, teams)

        captain_vc = get_captain_vc(date)
        points = calculate_fantasy_points(cd_scorecard)
        db.bulk_upsert_player_points(row["match_id"], points, captain_vc, force=True)
        db.set_enrichment_version(row["match_id"], "cd_v2")
        enriched += 1
        logger.info("CricketData enriched match %s (direct/assisted runouts + lbw_bowled)", row["match_id"])

    if enriched:
        logger.info("CricketData enrichment complete: %d matches updated", enriched)


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

    # Cricsheet first (free) — covers LBW/bowled + runouts without touching other scoring.
    try:
        _backfill_enrichment_from_cricsheet()
    except Exception as e:
        logger.error("Cricsheet enrichment error: %s", e)

    # CricketData fallback only for matches Cricsheet couldn't find (rare).
    try:
        _enrich_from_cricketdata()
    except Exception as e:
        logger.error("CricketData enrichment error: %s", e)

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

    # First-time seed must use Phase 1 names so historical match data
    # backfilled from remote attaches to the correct team_id, then we rename
    # to the Phase 2 display name. After that, reseed Phase 2 rosters.
    if team_count == 0:
        logger.info("No teams in DB — seeding Phase 1 teams (will rename to Phase 2 names)...")
        from teams_phase1 import TEAMS_PHASE1
        db.seed_teams(TEAMS_PHASE1)

    # Restore persisted data BEFORE the rename so old team_name → team_id
    # binding can still be resolved by the snapshot/restore code.
    if db.get_match_count() == 0:
        logger.info("No matches in DB — restoring from remote backup")
        if not db.restore_from_remote():
            logger.info("Remote restore failed — loading seed file as last resort")
            db.load_seed_data()
        logger.info("After restore: %d matches in DB", db.get_match_count())

    logger.info("DB has %d matches after restore", db.get_match_count())

    # Mid-season auction transition (idempotent across restarts):
    # 1. Close Phase 1 rosters (set removed_date if not already)
    # 2. Freeze Phase 1 top-11 totals if not already frozen
    # 3. Rename teams to Phase 2 display names (preserves team_id)
    # 4. Reseed Phase 2 rosters
    db.close_phase1_rosters()
    snapshot_summary = db.freeze_phase1_snapshot()
    logger.info("Phase 1 frozen totals: %s", snapshot_summary)
    db.rename_teams(TEAM_RENAME_MAP)
    from teams import TEAMS
    db.reseed_rosters(TEAMS, phase=2)

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

    # Daily Cricsheet enrichment: 2 AM IST = 20:30 UTC previous day.
    # Free (single ZIP download). Applies LBW/bowled + direct/assisted runout
    # deltas to any match not yet marked 'cricsheet' — corrects CD values
    # once Cricsheet publishes the ball-by-ball file (usually within hours).
    scheduler.add_job(
        _backfill_enrichment_from_cricsheet,
        CronTrigger(hour=20, minute=30),
        id="cricsheet_daily",
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
    """Latest match: per-team impact, rank movers, captain ROI, narratives."""
    latest = db.get_latest_match()
    if not latest:
        return {"match": None, "team_impacts": [], "rank_movers": [], "captain_roi": []}

    match_points = db.get_live_match_points(latest["match_id"])
    standings_data = db.get_standings()
    from teams import get_player_meta
    player_meta = get_player_meta()  # {player_name: {designation, fantasy_team, ipl_team, role}}

    # Build player → team_name (Phase 2 active rosters)
    player_to_team = {}
    for team in standings_data:
        for p in team["players"]:
            player_to_team[p["player_name"]] = team["team_name"]

    # Per-team impact aggregation (all this match's contributing players)
    team_impacts: dict = {}
    for pp in match_points:
        team_name = player_to_team.get(pp["player_name"])
        if not team_name:
            continue
        if team_name not in team_impacts:
            team_impacts[team_name] = {
                "team_name": team_name,
                "match_pts": 0,
                "players": [],
            }
        pp["display_name"] = get_display_name(pp["player_name"])
        # Tag designation so the UI can flag C/VC inline
        pp["designation"] = player_meta.get(pp["player_name"], {}).get("designation", "")
        team_impacts[team_name]["players"].append(pp)
        team_impacts[team_name]["match_pts"] += pp["total_pts"]

    # Per-team top-11 contribution from THIS match (the part that actually
    # affects standings). pts_history_full[-1] = latest match's contribution.
    top11_match_pts = {}
    for team in standings_data:
        hist = team.get("pts_history_full") or []
        top11_match_pts[team["team_name"]] = hist[-1] if hist else 0

    # Rank movers: rank now vs rank without this match's top-11 contribution
    rank_movers = _compute_rank_movers(standings_data, top11_match_pts)

    # Captain ROI: C and VC raw + bonus delivered per team (this match)
    captain_roi = _compute_captain_roi(team_impacts, player_meta)

    # Narrative one-liner per team
    for impact in team_impacts.values():
        impact["narrative"] = _build_team_narrative(impact)
        impact["top11_match_pts"] = top11_match_pts.get(impact["team_name"], 0)

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
        "rank_movers": rank_movers,
        "captain_roi": captain_roi,
    }


def _compute_rank_movers(standings_data: list[dict],
                          top11_match_pts: dict[str, int]) -> list[dict]:
    """Rank delta vs. before this match (using top-11 contribution).

    Returns sorted by absolute movement (biggest swings first).
    """
    # Current rank
    current_rank = {t["team_name"]: i + 1 for i, t in enumerate(standings_data)}

    # Rank if we strip this match's top-11 contribution
    snapshot = [
        {
            "team_name": t["team_name"],
            "before_pts": (t["total_pts"] or 0) - top11_match_pts.get(t["team_name"], 0),
        }
        for t in standings_data
    ]
    snapshot.sort(key=lambda x: x["before_pts"], reverse=True)
    prev_rank = {t["team_name"]: i + 1 for i, t in enumerate(snapshot)}

    movers = []
    for tname, cur in current_rank.items():
        prev = prev_rank.get(tname, cur)
        delta = prev - cur  # positive = moved up
        movers.append({
            "team_name": tname,
            "current_rank": cur,
            "prev_rank": prev,
            "delta": delta,
            "match_pts": top11_match_pts.get(tname, 0),
        })
    # Sort: biggest movers first (positive deltas at top), then by match_pts
    movers.sort(key=lambda x: (-abs(x["delta"]), -x["match_pts"]))
    return movers


def _compute_captain_roi(team_impacts: dict, player_meta: dict) -> list[dict]:
    """Per-team C+VC raw / bonus delivered for this match. Sorted by bonus."""
    rows = []
    for team_name, impact in team_impacts.items():
        c_player = vc_player = None
        for p in impact["players"]:
            d = p.get("designation")
            if d == "C":
                c_player = p
            elif d == "VC":
                vc_player = p
        c_raw = c_player["raw_pts"] if c_player else 0
        vc_raw = vc_player["raw_pts"] if vc_player else 0
        # Bonus = total - raw (the multiplier-driven extra)
        c_bonus = (c_player["total_pts"] - c_raw) if c_player else 0
        vc_bonus = (vc_player["total_pts"] - vc_raw) if vc_player else 0
        rows.append({
            "team_name": team_name,
            "captain_name": c_player["display_name"] if c_player else None,
            "captain_played": c_player is not None,
            "captain_raw": c_raw,
            "captain_bonus": c_bonus,
            "vc_name": vc_player["display_name"] if vc_player else None,
            "vc_played": vc_player is not None,
            "vc_raw": vc_raw,
            "vc_bonus": vc_bonus,
            "total_bonus": c_bonus + vc_bonus,
        })
    rows.sort(key=lambda x: x["total_bonus"], reverse=True)
    return rows


def _build_team_narrative(impact: dict) -> str:
    """One snappy line summarising this team's performance this match.

    Picks ONE angle (most interesting) and renders it.
    """
    players = impact["players"]
    if not players:
        return "No fantasy players in this match."

    total = impact["match_pts"]
    captain = next((p for p in players if p.get("designation") == "C"), None)
    vc = next((p for p in players if p.get("designation") == "VC"), None)
    sorted_pts = sorted(players, key=lambda p: p["total_pts"], reverse=True)
    top = sorted_pts[0]
    ducks = sum(1 for p in players if p["raw_pts"] <= 0)
    twentys = sum(1 for p in players if p["total_pts"] >= 20)

    # Priority 1: Captain went off (≥50 raw)
    if captain and captain["raw_pts"] >= 50:
        return f"🔥 {captain['display_name']} (C) destroyed — {captain['raw_pts']} raw, +{captain['total_pts'] - captain['raw_pts']} bonus"

    # Priority 2: Captain duck/flop (raw ≤ 5, but did play)
    if captain and captain["raw_pts"] <= 5:
        return f"💀 {captain['display_name']} (C) flopped — only {captain['raw_pts']} raw, captain bonus wasted"

    # Priority 3: One player carried (≥35% of team total, with total ≥ 50)
    if total >= 50 and (top["total_pts"] / total) >= 0.35:
        pct = round((top["total_pts"] / total) * 100)
        return f"🎒 {top['display_name']} carried — {top['total_pts']}/{total} ({pct}%)"

    # Priority 4: Lots of ducks
    if ducks >= 4:
        return f"🪦 Brutal — {ducks} ducks, top scorer only {top['total_pts']}"

    # Priority 5: Balanced spread
    if twentys >= 4:
        return f"🤝 Balanced — {twentys} players in 20+"

    # Default: quiet match
    return f"Quiet match — top scorer {top['display_name']} with {top['total_pts']}"


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


# Public, cooldown-rate-limited refresh used by the Live tab button.
_last_public_refresh = {"at": 0.0}
PUBLIC_REFRESH_COOLDOWN_SEC = 20


@app.post("/api/refresh-live")
async def refresh_live_public():
    """Public on-demand refresh for the Live tab.

    Triggers match discovery (catches today's match if cron hasn't run yet)
    and forces an ESPN refresh of any in-progress match. Rate-limited per
    process so users mashing the button don't hammer ESPN/CD.
    """
    import time as _time
    now = _time.time()
    elapsed = now - _last_public_refresh["at"]
    if elapsed < PUBLIC_REFRESH_COOLDOWN_SEC:
        return {
            "status": "cooldown",
            "retry_in_sec": int(PUBLIC_REFRESH_COOLDOWN_SEC - elapsed),
        }
    _last_public_refresh["at"] = now
    try:
        fetch_and_store_matches()
        # ESPN refresh is internally time-gated; we bypass that here so the
        # button works regardless of clock by calling get_espn_scorecard
        # directly for any in_progress matches.
        conn = db.get_db()
        rows = conn.execute(
            "SELECT match_id, date, teams_json FROM matches WHERE status = 'in_progress'"
        ).fetchall()
        conn.close()
        for row in rows:
            try:
                teams = json.loads(row["teams_json"]) if row["teams_json"] else []
                date = row["date"][:10] if row["date"] else ""
                captain_vc = get_captain_vc(date)
                scorecard = get_espn_scorecard(date, teams)
                if not scorecard:
                    continue
                scorecard.match_id = row["match_id"]
                _inject_enrichment(scorecard, _get_existing_enrichment(row["match_id"]))
                points = calculate_fantasy_points(scorecard)
                db.bulk_upsert_player_points(row["match_id"], points, captain_vc, force=True)
            except Exception as e:
                logger.error("Public refresh ESPN error for %s: %s", row["match_id"], e)
        return {"status": "refreshed", "in_progress_count": len(rows)}
    except Exception as e:
        logger.error("Public refresh error: %s", e)
        raise HTTPException(500, f"refresh failed: {e}")


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


@app.post("/api/admin/freeze-phase1")
async def freeze_phase1(request: Request):
    """Manually trigger the Phase 1 snapshot freeze (idempotent)."""
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Invalid secret")
    summary = db.freeze_phase1_snapshot()
    return {"status": "frozen", "snapshot": summary}


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
