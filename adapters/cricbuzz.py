"""Adapter for Cricbuzz Cricket API via RapidAPI (live + completed match data)."""
import re
import json
import os
import httpx
import logging
from datetime import datetime, timezone
from adapters.base import DataSourceAdapter
from models import MatchScorecard, BattingEntry, BowlingEntry, FieldingEntry
from config import CRICBUZZ_API_KEY, CRICBUZZ_MONTHLY_LIMIT
from name_mapping import get_display_name

logger = logging.getLogger(__name__)

BASE_URL = "https://cricbuzz-cricket.p.rapidapi.com"
IPL_SERIES_FILTER = "Indian Premier League"

# --- API call tracking (file-persisted across restarts) ---
_CALL_LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "api_call_log.json")


def _get_call_log() -> dict:
    try:
        with open(_CALL_LOG_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"month": "", "calls": 0}


def _save_call_log(log: dict):
    os.makedirs(os.path.dirname(_CALL_LOG_PATH), exist_ok=True)
    with open(_CALL_LOG_PATH, "w") as f:
        json.dump(log, f)


def get_api_usage() -> dict:
    """Return current API usage stats for UI display."""
    log = _get_call_log()
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    calls = log["calls"] if log["month"] == current_month else 0
    return {"month": current_month, "calls_used": calls, "limit": CRICBUZZ_MONTHLY_LIMIT}


def _increment_call_count() -> bool:
    """Increment call counter. Returns False if limit reached."""
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    log = _get_call_log()
    if log["month"] != current_month:
        log = {"month": current_month, "calls": 0}
    if log["calls"] >= CRICBUZZ_MONTHLY_LIMIT:
        return False
    log["calls"] += 1
    _save_call_log(log)
    return True


class CricbuzzAdapter(DataSourceAdapter):
    def __init__(self, api_key: str = ""):
        self.api_key = api_key or CRICBUZZ_API_KEY
        self.headers = {
            "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com",
            "x-rapidapi-key": self.api_key,
            "Content-Type": "application/json",
        }

    def _get(self, path: str, params: dict = None) -> dict | None:
        if not _increment_call_count():
            usage = get_api_usage()
            logger.warning(
                "Cricbuzz API monthly limit reached (%d/%d) — skipping call to %s",
                usage["calls_used"], usage["limit"], path,
            )
            return None
        try:
            resp = httpx.get(
                f"{BASE_URL}/{path}",
                headers=self.headers,
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and "message" in data:
                msg = str(data["message"])
                if "does not exist" in msg or "not subscribed" in msg:
                    logger.warning("Cricbuzz API error: %s", msg)
                    return None
            return data
        except Exception as e:
            logger.error("Cricbuzz API error on %s: %s", path, e)
            return None

    # ------------------------------------------------------------------
    # Match discovery
    # ------------------------------------------------------------------

    def _extract_ipl_matches(self, data: dict) -> list[dict]:
        """Extract IPL matches from the typeMatches response structure."""
        matches = []
        if not data:
            return matches
        for type_group in data.get("typeMatches", []):
            for series_group in type_group.get("seriesMatches", []):
                wrapper = series_group.get("seriesAdWrapper", {})
                if not wrapper:
                    continue
                series_name = wrapper.get("seriesName", "")
                if IPL_SERIES_FILTER not in series_name:
                    continue
                for match_wrapper in wrapper.get("matches", []):
                    mi = match_wrapper.get("matchInfo", {})
                    state = (mi.get("state") or "").lower()
                    if state == "complete":
                        status = "complete"
                    elif state in ("in progress", "innings break",
                                   "toss", "stumps", "lunch", "tea"):
                        status = "in_progress"
                    else:
                        status = "upcoming"

                    team1 = mi.get("team1", {}).get("teamName", "")
                    team2 = mi.get("team2", {}).get("teamName", "")

                    # Epoch ms → date string
                    date_str = ""
                    start_ts = mi.get("startDate")
                    if start_ts:
                        try:
                            from datetime import datetime, timezone
                            dt = datetime.fromtimestamp(
                                int(start_ts) / 1000, tz=timezone.utc
                            )
                            date_str = dt.strftime("%Y-%m-%d")
                        except (ValueError, OSError):
                            pass

                    venue = ""
                    vi = mi.get("venueInfo") or {}
                    if vi:
                        venue = f"{vi.get('ground', '')}, {vi.get('city', '')}"

                    matches.append({
                        "match_id": str(mi.get("matchId", "")),
                        "date": date_str,
                        "teams": [team1, team2],
                        "venue": venue,
                        "status": status,
                        "name": mi.get("matchDesc", ""),
                    })
        return matches

    def get_match_list(self, season: str = "") -> list[dict]:
        """Get IPL matches from both recent and live endpoints."""
        all_matches: list[dict] = []
        seen_ids: set[str] = set()

        for endpoint in ("matches/v1/recent", "matches/v1/live"):
            data = self._get(endpoint)
            if not data:
                continue
            for m in self._extract_ipl_matches(data):
                if m["match_id"] not in seen_ids:
                    all_matches.append(m)
                    seen_ids.add(m["match_id"])

        return all_matches

    def get_current_matches(self) -> list[dict]:
        """Get currently live IPL matches only."""
        data = self._get("matches/v1/live")
        if not data:
            return []
        return [
            m for m in self._extract_ipl_matches(data)
            if m["status"] == "in_progress"
        ]

    # ------------------------------------------------------------------
    # Scorecard
    # ------------------------------------------------------------------

    def get_scorecard(self, match_id: str, date: str = "",
                      teams: list[str] | None = None) -> MatchScorecard | None:
        """Fetch full scorecard and parse into MatchScorecard.

        Tries the RapidAPI first; on failure (e.g. 429), falls back to
        scraping the Cricbuzz mobile site.  In both cases, bowling dots
        are enriched from the free ESPN API.

        API response per innings:
          batsman[]: name, runs, balls, fours, sixes, outdec, iscaptain, iskeeper
          bowler[]:  name, overs, maidens, wickets, runs, economy, dots(=0), balls
          extras, fow, score, wickets, overs, batteamname
        """
        data = self._get(f"mcenter/v1/{match_id}/hscard")
        if not data or "scorecard" not in data:
            logger.info("API failed for %s — falling back to web scrape", match_id)
            result = scrape_scorecard(match_id)
        else:
            result = self._parse_api_scorecard(data, match_id)

        if result:
            _enrich_bowling_dots_espn(result, date, teams or [])
        return result

    def _parse_api_scorecard(self, data: dict, match_id: str) -> MatchScorecard | None:
        """Parse the RapidAPI hscard response into a MatchScorecard."""
        if "scorecard" not in data:
            return None

        scorecard_innings = data["scorecard"]
        if not scorecard_innings:
            logger.info("No scorecard data for match %s", match_id)
            return None

        playing_xi: set[str] = set()
        batting: dict[str, BattingEntry] = {}
        bowling: dict[str, BowlingEntry] = {}
        fielding: dict[str, FieldingEntry] = {}
        batters_who_batted: set[str] = set()
        teams: list[str] = []
        # Collect dismissals per innings so we process them after bowling
        all_dismissals: list[str] = []

        for innings_data in scorecard_innings:
            team_name = innings_data.get("batteamname", "")
            if team_name and team_name not in teams:
                teams.append(team_name)

            # --- Batting ---
            for b in innings_data.get("batsman", []):
                name = b.get("name", "")
                if not name:
                    continue
                playing_xi.add(name)

                outdec = (b.get("outdec") or "").strip()
                is_dismissed = outdec != "" and outdec.lower() != "not out"

                runs = b.get("runs", 0) or 0
                balls = b.get("balls", 0) or 0
                fours = b.get("fours", 0) or 0
                sixes = b.get("sixes", 0) or 0

                if name in batting:
                    existing = batting[name]
                    existing.runs += runs
                    existing.balls += balls
                    existing.fours += fours
                    existing.sixes += sixes
                    existing.dismissed = existing.dismissed or is_dismissed
                else:
                    batting[name] = BattingEntry(
                        player=name, runs=runs, balls=balls,
                        fours=fours, sixes=sixes, dismissed=is_dismissed,
                    )

                if balls > 0 or runs > 0 or is_dismissed:
                    batters_who_batted.add(name)

                if is_dismissed and outdec:
                    all_dismissals.append(outdec)

            # --- Bowling ---
            for bw in innings_data.get("bowler", []):
                name = bw.get("name", "")
                if not name:
                    continue
                playing_xi.add(name)

                overs_str = str(bw.get("overs", "0"))
                total_balls = _overs_to_balls(overs_str)
                maidens = bw.get("maidens", 0) or 0
                runs = bw.get("runs", 0) or 0
                wickets = bw.get("wickets", 0) or 0

                # Synthetic overs_detail so scoring.py can count maidens
                overs_detail: dict = {}
                for mi in range(maidens):
                    overs_detail[f"maiden_{name}_{mi}"] = {
                        "balls": 6, "runs": 0,
                    }

                if name in bowling:
                    existing = bowling[name]
                    existing.balls += total_balls
                    existing.runs += runs
                    existing.wickets += wickets
                    existing.overs_detail.update(overs_detail)
                else:
                    bowling[name] = BowlingEntry(
                        player=name, balls=total_balls, runs=runs,
                        wickets=wickets, dots=0, lbw_bowled=0,
                        overs_detail=overs_detail,
                    )

        # --- Fielding + lbw/bowled from dismissal text ---
        for outdec in all_dismissals:
            _parse_dismissal(outdec, fielding, bowling)

        # --- Determine match status from available innings ---
        if len(scorecard_innings) >= 2:
            match_status = "complete"
        elif len(scorecard_innings) == 1:
            match_status = "in_progress"
        else:
            match_status = "upcoming"

        # --- Normalize all player names ---
        playing_xi = {get_display_name(n) for n in playing_xi}
        batting = {get_display_name(k): v for k, v in batting.items()}
        bowling = {get_display_name(k): v for k, v in bowling.items()}
        fielding = {get_display_name(k): v for k, v in fielding.items()}
        batters_who_batted = {get_display_name(n) for n in batters_who_batted}

        return MatchScorecard(
            match_id=str(match_id),
            date="",   # supplied by get_match_list()
            teams=teams,
            venue="",  # supplied by get_match_list()
            status=match_status,
            playing_xi=playing_xi,
            batting=batting,
            bowling=bowling,
            fielding=fielding,
            batters_who_batted=batters_who_batted,
        )


# ------------------------------------------------------------------
# Helpers (module-level so they're testable)
# ------------------------------------------------------------------

def _overs_to_balls(overs_str: str) -> int:
    """Convert overs string ('4' or '3.2') to total legal deliveries."""
    try:
        if "." in overs_str:
            full, partial = overs_str.split(".", 1)
            return int(full) * 6 + int(partial)
        return int(float(overs_str)) * 6
    except (ValueError, TypeError):
        return 0


def _parse_dismissal(
    outdec: str,
    fielding: dict[str, FieldingEntry],
    bowling: dict[str, BowlingEntry],
) -> None:
    """Parse dismissal text to extract fielding credits and lbw/bowled bonuses.

    Known formats from Cricbuzz:
      c Fielder b Bowler      — caught
      c & b Bowler            — caught-and-bowled
      b Bowler                — bowled
      lbw b Bowler            — LBW
      st Keeper b Bowler      — stumped
      run out (Fielder)       — run out (single fielder)
      run out (F1/F2)         — run out (multiple fielders)
      hit wicket b Bowler     — hit wicket (no fielding credit)
      retired hurt            — no credit
      not out / empty         — no credit
    """
    text = outdec.strip()
    if not text or text.lower() == "not out":
        return

    # --- Caught and bowled: "c & b Bowlername" ---
    m = re.match(r"^c\s*&\s*b\s+(.+)$", text, re.IGNORECASE)
    if m:
        bowler = m.group(1).strip()
        _add_catch(fielding, bowler)
        return

    # --- Caught: "c Fieldername b Bowlername" ---
    m = re.match(r"^c\s+(.+?)\s+b\s+(.+)$", text, re.IGNORECASE)
    if m:
        catcher = m.group(1).strip()
        _add_catch(fielding, catcher)
        return

    # --- LBW: "lbw b Bowlername" ---
    m = re.match(r"^lbw\s+b\s+(.+)$", text, re.IGNORECASE)
    if m:
        bowler = m.group(1).strip()
        _add_lbw_bowled(bowling, bowler)
        return

    # --- Bowled: "b Bowlername" ---
    m = re.match(r"^b\s+(.+)$", text, re.IGNORECASE)
    if m:
        bowler = m.group(1).strip()
        _add_lbw_bowled(bowling, bowler)
        return

    # --- Stumped: "st Keepername b Bowlername" ---
    m = re.match(r"^st\s+(.+?)\s+b\s+(.+)$", text, re.IGNORECASE)
    if m:
        keeper = m.group(1).strip()
        _add_stumping(fielding, keeper)
        return

    # --- Run out: "run out (Fielder)" or "run out (F1/F2)" ---
    m = re.match(r"^run\s+out\s*\(([^)]+)\)", text, re.IGNORECASE)
    if m:
        fielders_str = m.group(1).strip()
        for name in fielders_str.split("/"):
            name = name.strip()
            if name:
                _add_runout(fielding, name)
        return

    # hit wicket, retired hurt, etc. — no fielding credit
    logger.debug("Unhandled dismissal format: %s", text)


def _add_catch(fielding: dict[str, FieldingEntry], name: str) -> None:
    if name not in fielding:
        fielding[name] = FieldingEntry(player=name)
    fielding[name].catches += 1


def _add_stumping(fielding: dict[str, FieldingEntry], name: str) -> None:
    if name not in fielding:
        fielding[name] = FieldingEntry(player=name)
    fielding[name].stumpings += 1


def _add_runout(fielding: dict[str, FieldingEntry], name: str) -> None:
    if name not in fielding:
        fielding[name] = FieldingEntry(player=name)
    fielding[name].runouts += 1


def _add_lbw_bowled(bowling: dict[str, BowlingEntry], bowler: str) -> None:
    if bowler in bowling:
        bowling[bowler].lbw_bowled += 1
    else:
        logger.warning(
            "LBW/bowled credit for bowler '%s' but no bowling entry found", bowler
        )


# ------------------------------------------------------------------
# Web scraper — Cricbuzz mobile site (no API key, no rate limits)
# ------------------------------------------------------------------

def _fetch_mobile_scorecard_json(match_id: str) -> list[dict] | None:
    """Fetch and extract the scorecard JSON embedded in the Cricbuzz mobile page."""
    url = f"https://m.cricbuzz.com/live-cricket-scorecard/{match_id}"
    try:
        resp = httpx.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0)"},
            follow_redirects=True, timeout=30,
        )
        if resp.status_code != 200:
            return None
    except Exception as e:
        logger.error("Failed to fetch Cricbuzz mobile page: %s", e)
        return None

    html = resp.text

    # Extract batting entries: {"batName":"Name",...,"dots":N,...}
    bat_pattern = (
        r'\\"batId\\":(\d+),\\"batName\\":\\"([^\\]+)\\",'
        r'\\"batShortName\\":\\"[^\\]*\\",'
        r'\\"isCaptain\\":(true|false),\\"isKeeper\\":(true|false),'
        r'\\"runs\\":(\d+),\\"balls\\":(\d+),\\"dots\\":(\d+),'
        r'\\"fours\\":(\d+),\\"sixes\\":(\d+),'
        r'\\"mins\\":\d+,\\"strikeRate\\":[0-9.]+,'
        r'\\"outDesc\\":\\"([^\\]*)\\"'
    )

    # Extract bowling entries (field is bowlerId, not bowlId)
    bowl_pattern = (
        r'\\"bowlerId\\":(\d+),\\"bowlName\\":\\"([^\\]+)\\",'
        r'\\"bowlShortName\\":\\"[^\\]*\\",'
        r'\\"isCaptain\\":(true|false),\\"isKeeper\\":(true|false),'
        r'\\"overs\\":([0-9.]+),\\"maidens\\":(\d+),'
        r'\\"runs\\":(\d+),\\"wickets\\":(\d+),'
        r'\\"economy\\":[0-9.]+,'
        r'\\"no_balls\\":(\d+),\\"wides\\":(\d+),\\"dots\\":(\d+),'
        r'\\"balls\\":(\d+)'
    )

    bat_matches = re.findall(bat_pattern, html)
    bowl_matches = re.findall(bowl_pattern, html)

    if not bat_matches and not bowl_matches:
        logger.warning("No scorecard data found in mobile page for %s", match_id)
        return None

    return {"batting": bat_matches, "bowling": bowl_matches}


def scrape_scorecard(match_id: str) -> MatchScorecard | None:
    """Scrape the Cricbuzz mobile site to build a MatchScorecard.

    This is the fallback when the RapidAPI is rate-limited.
    Batting dots are accurate; bowling dots are estimated proportionally.
    """
    raw = _fetch_mobile_scorecard_json(match_id)
    if not raw:
        return None

    playing_xi: set[str] = set()
    batting: dict[str, BattingEntry] = {}
    bowling: dict[str, BowlingEntry] = {}
    fielding: dict[str, FieldingEntry] = {}
    batters_who_batted: set[str] = set()
    all_dismissals: list[str] = []
    total_bat_dots = 0

    # --- Parse batting ---
    for m in raw["batting"]:
        bat_id, name, is_capt, is_keeper, runs, balls, dots, fours, sixes, outdec = m
        runs, balls, dots, fours, sixes = int(runs), int(balls), int(dots), int(fours), int(sixes)
        playing_xi.add(name)
        is_dismissed = outdec != "" and outdec.lower() != "not out"

        if name in batting:
            e = batting[name]
            e.runs += runs
            e.balls += balls
            e.fours += fours
            e.sixes += sixes
            e.dismissed = e.dismissed or is_dismissed
        else:
            batting[name] = BattingEntry(
                player=name, runs=runs, balls=balls,
                fours=fours, sixes=sixes, dismissed=is_dismissed,
            )

        if balls > 0 or runs > 0 or is_dismissed:
            batters_who_batted.add(name)

        total_bat_dots += dots

        if is_dismissed and outdec:
            all_dismissals.append(outdec)

    # --- Parse bowling ---
    total_bowl_balls = 0
    for m in raw["bowling"]:
        bowl_id, name, is_capt, is_keeper, overs, maidens, runs, wickets, nb, wides, dots, balls_raw = m
        total_balls = _overs_to_balls(str(overs))
        maidens = int(maidens)
        runs = int(runs)
        wickets = int(wickets)
        playing_xi.add(name)

        overs_detail: dict = {}
        for mi in range(maidens):
            overs_detail[f"maiden_{name}_{mi}"] = {"balls": 6, "runs": 0}

        if name in bowling:
            e = bowling[name]
            e.balls += total_balls
            e.runs += runs
            e.wickets += wickets
            e.overs_detail.update(overs_detail)
        else:
            bowling[name] = BowlingEntry(
                player=name, balls=total_balls, runs=runs,
                wickets=wickets, dots=0, lbw_bowled=0,
                overs_detail=overs_detail,
            )
        total_bowl_balls += total_balls

    # Bowling dots are left at 0 here — ESPN enrichment fills them accurately

    # --- Fielding + lbw/bowled from dismissals ---
    for outdec in all_dismissals:
        _parse_dismissal(outdec, fielding, bowling)

    # --- Normalize names ---
    playing_xi = {get_display_name(n) for n in playing_xi}
    batting = {get_display_name(k): v for k, v in batting.items()}
    bowling = {get_display_name(k): v for k, v in bowling.items()}
    fielding = {get_display_name(k): v for k, v in fielding.items()}
    batters_who_batted = {get_display_name(n) for n in batters_who_batted}

    return MatchScorecard(
        match_id=str(match_id),
        date="", teams=[], venue="",
        status="complete",
        playing_xi=playing_xi,
        batting=batting,
        bowling=bowling,
        fielding=fielding,
        batters_who_batted=batters_who_batted,
    )


def _enrich_bowling_dots_espn(
    scorecard: MatchScorecard, date: str, teams: list[str],
) -> None:
    """Enrich bowling dots from the free ESPN API (accurate per-bowler dots)."""
    total_existing = sum(e.dots for e in scorecard.bowling.values())
    if total_existing > 0:
        return  # already have dots (e.g., from Cricsheet re-scoring)

    from adapters.espn import find_espn_event_id, fetch_espn_bowling_dots

    espn_id = find_espn_event_id(date, teams) if date else None
    if not espn_id:
        logger.info("Could not find ESPN event for dots enrichment (date=%s)", date)
        return

    dots_map = fetch_espn_bowling_dots(espn_id)
    if not dots_map:
        return

    enriched = 0
    for name, entry in scorecard.bowling.items():
        if name in dots_map:
            entry.dots = dots_map[name]
            enriched += 1

    logger.info(
        "Enriched %d/%d bowlers with ESPN dots (event %s)",
        enriched, len(scorecard.bowling), espn_id,
    )
