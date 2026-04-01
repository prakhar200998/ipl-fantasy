"""Adapter for CricketData.org API (live + historical match data)."""
import httpx
import logging
from datetime import datetime, timezone
from adapters.base import DataSourceAdapter
from models import MatchScorecard, BattingEntry, BowlingEntry, FieldingEntry
from config import CRICKETDATA_API_KEY, CRICKETDATA_DAILY_LIMIT, IPL_2026_SERIES_ID
from name_mapping import get_display_name

logger = logging.getLogger(__name__)

BASE_URL = "https://api.cricapi.com/v1"

# --- Daily API call tracking (in-memory, resets on redeploy) ---
_daily_call_log: dict = {"date": "", "calls": 0}


def _check_daily_limit() -> bool:
    """Check and increment daily call counter. Returns False if limit reached."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _daily_call_log["date"] != today:
        _daily_call_log["date"] = today
        _daily_call_log["calls"] = 0
    if _daily_call_log["calls"] >= CRICKETDATA_DAILY_LIMIT:
        logger.warning(
            "CricketData daily limit reached (%d/%d) — skipping API call",
            _daily_call_log["calls"], CRICKETDATA_DAILY_LIMIT,
        )
        return False
    _daily_call_log["calls"] += 1
    return True


def get_daily_usage() -> dict:
    """Return current daily API usage stats for UI display."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    calls = _daily_call_log["calls"] if _daily_call_log["date"] == today else 0
    return {"date": today, "calls_used": calls, "limit": CRICKETDATA_DAILY_LIMIT}


class CricketDataAdapter(DataSourceAdapter):
    def __init__(self, api_key: str = ""):
        self.api_key = api_key or CRICKETDATA_API_KEY

    def _get(self, endpoint: str, params: dict = None) -> dict | None:
        if not _check_daily_limit():
            return None
        params = params or {}
        params["apikey"] = self.api_key
        try:
            resp = httpx.get(f"{BASE_URL}/{endpoint}", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "success":
                logger.warning("CricketData API error: %s — %s", data.get("status"), data.get("reason", "unknown"))
                return None
            return data
        except Exception as e:
            logger.error("CricketData API error: %s", e)
            return None

    def get_match_list(self, season: str) -> list[dict]:
        """Get IPL 2026 matches from the series info endpoint."""
        data = self._get("series_info", {"id": IPL_2026_SERIES_ID})
        if not data or "data" not in data:
            return []

        match_list = data["data"].get("matchList", [])
        matches = []
        for m in match_list:
            if m.get("matchEnded"):
                status = "complete"
            elif m.get("matchStarted"):
                status = "in_progress"
            else:
                status = "upcoming"
            matches.append({
                "match_id": m["id"],
                "date": m.get("date", ""),
                "teams": m.get("teams", []),
                "venue": m.get("venue", ""),
                "status": status,
                "name": m.get("name", ""),
            })
        return matches

    def get_current_matches(self) -> list[dict]:
        """Get currently live matches from currentMatches endpoint."""
        data = self._get("currentMatches", {"offset": 0})
        if not data:
            return []
        matches = []
        for m in data.get("data", []):
            if "Indian Premier League" not in m.get("name", ""):
                continue
            if m.get("matchEnded"):
                status = "complete"
            elif m.get("matchStarted"):
                status = "in_progress"
            else:
                status = "upcoming"
            matches.append({
                "match_id": m["id"],
                "date": m.get("date", ""),
                "teams": m.get("teams", []),
                "venue": m.get("venue", ""),
                "status": status,
                "name": m.get("name", ""),
            })
        return matches

    def get_scorecard(self, match_id: str) -> MatchScorecard | None:
        """Fetch scorecard from API and parse into MatchScorecard.

        API response structure:
          data.scorecard[] — one entry per innings
            batting[]: batsman.name, r, b, 4s, 6s, sr, dismissal, dismissal-text, bowler.name, catcher.name
            bowling[]: bowler.name, o, m, r, w, nb, wd, eco
            catching[]: catcher.name, catch, stumped, runout, cb, lbw, bowled
          data.matchStarted, data.matchEnded, data.teams[], data.venue, data.date
        """
        data = self._get("match_scorecard", {"id": match_id})
        if not data or "data" not in data:
            return None

        match_data = data["data"]
        scorecard = match_data.get("scorecard", [])
        if not scorecard:
            logger.info("No scorecard data for match %s", match_id)
            return None

        playing_xi = set()
        batting = {}
        bowling = {}
        fielding = {}
        batters_who_batted = set()

        for innings_data in scorecard:
            # --- Parse batting ---
            for b in innings_data.get("batting", []):
                name = b.get("batsman", {}).get("name", "")
                if not name:
                    continue
                playing_xi.add(name)

                # "dismissal" key is ABSENT for not-out batsmen
                dismissal_type = b.get("dismissal", "")
                is_dismissed = dismissal_type != "" and dismissal_type != "not out"

                runs = b.get("r", 0)
                balls = b.get("b", 0)
                fours = b.get("4s", 0)
                sixes = b.get("6s", 0)

                entry = BattingEntry(
                    player=name,
                    runs=runs,
                    balls=balls,
                    fours=fours,
                    sixes=sixes,
                    dismissed=is_dismissed,
                )

                # Accumulate if player batted in both innings (shouldn't happen in T20, but safe)
                if name in batting:
                    existing = batting[name]
                    existing.runs += runs
                    existing.balls += balls
                    existing.fours += fours
                    existing.sixes += sixes
                    existing.dismissed = existing.dismissed or is_dismissed
                else:
                    batting[name] = entry

                if balls > 0 or runs > 0 or is_dismissed:
                    batters_who_batted.add(name)

            # --- Parse bowling ---
            for bw in innings_data.get("bowling", []):
                name = bw.get("bowler", {}).get("name", "")
                if not name:
                    continue
                playing_xi.add(name)

                overs_float = bw.get("o", 0)
                full_overs = int(overs_float)
                extra_balls = round((overs_float - full_overs) * 10)
                total_balls = full_overs * 6 + extra_balls

                maidens = bw.get("m", 0)
                wickets = bw.get("w", 0)

                # "r" may exclude extras (wides/no-balls). Use "eco" to
                # derive true total conceded, falling back to r + wd + nb.
                eco = bw.get("eco", 0) or 0
                if eco and total_balls:
                    runs = round(float(eco) * total_balls / 6)
                else:
                    # Fallback: add wide/no-ball penalty runs to base runs
                    runs = int(bw.get("r", 0) or 0) + int(bw.get("wd", 0) or 0) + int(bw.get("nb", 0) or 0)
                    logger.info("No eco field for %s — using r+wd+nb=%d", name, runs)

                # Build overs_detail for maiden detection
                # The API gives us total maidens directly via "m" field
                # We'll store a synthetic overs_detail so scoring.py can count maidens
                overs_detail = {}
                for mi in range(maidens):
                    overs_detail[f"maiden_{name}_{mi}"] = {"balls": 6, "runs": 0}

                if name in bowling:
                    existing = bowling[name]
                    existing.balls += total_balls
                    existing.runs += runs
                    existing.wickets += wickets
                    existing.overs_detail.update(overs_detail)
                else:
                    bowling[name] = BowlingEntry(
                        player=name,
                        balls=total_balls,
                        runs=runs,
                        wickets=wickets,
                        dots=0,  # API doesn't provide dot balls
                        lbw_bowled=0,  # Will be populated from catching array
                        overs_detail=overs_detail,
                    )

            # --- Parse catching/fielding ---
            # The catching array includes ALL fielding contributions:
            # catches, stumpings, runouts, AND bowler's lbw/bowled credits
            for c in innings_data.get("catching", []):
                name = c.get("catcher", {}).get("name", "")
                if not name:
                    continue
                playing_xi.add(name)

                catches = c.get("catch", 0)
                stumpings = c.get("stumped", 0)
                runouts = c.get("runout", 0)
                cb = c.get("cb", 0)  # caught and bowled
                lbw_count = c.get("lbw", 0)
                bowled_count = c.get("bowled", 0)

                # Caught-and-bowled counts as a catch for the bowler
                total_catches = catches + cb

                # Add fielding credit for catches, stumpings, runouts
                if total_catches > 0 or stumpings > 0 or runouts > 0:
                    if name in fielding:
                        fielding[name].catches += total_catches
                        fielding[name].stumpings += stumpings
                        fielding[name].runouts += runouts
                    else:
                        fielding[name] = FieldingEntry(
                            player=name,
                            catches=total_catches,
                            stumpings=stumpings,
                            runouts=runouts,
                        )

                # Add lbw/bowled bonus for bowlers
                lbw_bowled_total = lbw_count + bowled_count + cb
                if lbw_bowled_total > 0 and name in bowling:
                    bowling[name].lbw_bowled += lbw_bowled_total

        # Extract full playing XI from teamInfo (catches players with zero
        # involvement who don't appear in batting/bowling/catching arrays)
        for team_info in match_data.get("teamInfo", []):
            for p in team_info.get("players", []):
                name = p.get("name", "")
                if name:
                    playing_xi.add(name)

        # Determine match status
        if match_data.get("matchEnded"):
            status = "complete"
        elif match_data.get("matchStarted"):
            status = "in_progress"
        else:
            status = "upcoming"

        teams = match_data.get("teams", [])

        # Normalize all player names to display names (so they match roster)
        playing_xi = {get_display_name(n) for n in playing_xi}
        batting = {get_display_name(k): v for k, v in batting.items()}
        bowling = {get_display_name(k): v for k, v in bowling.items()}
        fielding = {get_display_name(k): v for k, v in fielding.items()}
        batters_who_batted = {get_display_name(n) for n in batters_who_batted}

        return MatchScorecard(
            match_id=match_id,
            date=match_data.get("date", ""),
            teams=teams,
            venue=match_data.get("venue", ""),
            status=status,
            playing_xi=playing_xi,
            batting=batting,
            bowling=bowling,
            fielding=fielding,
            batters_who_batted=batters_who_batted,
        )
