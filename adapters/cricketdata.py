"""Adapter for CricketData.org API (live match data)."""
import httpx
import logging
from adapters.base import DataSourceAdapter
from models import MatchScorecard, BattingEntry, BowlingEntry, FieldingEntry
from config import CRICKETDATA_API_KEY

logger = logging.getLogger(__name__)

BASE_URL = "https://api.cricapi.com/v1"


class CricketDataAdapter(DataSourceAdapter):
    def __init__(self, api_key: str = ""):
        self.api_key = api_key or CRICKETDATA_API_KEY

    def _get(self, endpoint: str, params: dict = None) -> dict | None:
        params = params or {}
        params["apikey"] = self.api_key
        try:
            resp = httpx.get(f"{BASE_URL}/{endpoint}", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "success":
                logger.warning("API error: %s", data.get("status"))
                return None
            return data
        except Exception as e:
            logger.error("CricketData API error: %s", e)
            return None

    def get_match_list(self, season: str) -> list[dict]:
        """Get current/recent IPL matches."""
        data = self._get("currentMatches", {"offset": 0})
        if not data:
            return []
        matches = []
        for m in data.get("data", []):
            if "Indian Premier League" not in m.get("name", ""):
                continue
            matches.append({
                "match_id": m["id"],
                "date": m.get("date", ""),
                "teams": m.get("teams", []),
                "venue": m.get("venue", ""),
                "status": "in_progress" if m.get("matchStarted") and not m.get("matchEnded") else
                         "complete" if m.get("matchEnded") else "upcoming",
            })
        return matches

    def get_scorecard(self, match_id: str) -> MatchScorecard | None:
        """Fetch scorecard from API and parse into MatchScorecard."""
        data = self._get("match_scorecard", {"id": match_id})
        if not data or "data" not in data:
            return None

        match_data = data["data"]
        scorecard = match_data.get("scorecard", [])

        playing_xi = set()
        batting = {}
        bowling = {}
        fielding = {}
        batters_who_batted = set()

        for innings_data in scorecard:
            # Parse batting
            for b in innings_data.get("batting", []):
                name = b["batsman"]["name"]
                playing_xi.add(name)
                entry = BattingEntry(
                    player=name,
                    runs=b.get("r", 0),
                    balls=b.get("b", 0),
                    fours=b.get("4s", 0),
                    sixes=b.get("6s", 0),
                    dismissed=b.get("dismissal", "") != "not out",
                )
                batting[name] = entry
                if entry.balls > 0 or entry.runs > 0 or entry.dismissed:
                    batters_who_batted.add(name)

            # Parse bowling
            for bw in innings_data.get("bowling", []):
                name = bw["bowler"]["name"]
                playing_xi.add(name)
                overs_float = bw.get("o", 0)
                full_overs = int(overs_float)
                extra_balls = round((overs_float - full_overs) * 10)
                total_balls = full_overs * 6 + extra_balls

                entry = BowlingEntry(
                    player=name,
                    balls=total_balls,
                    runs=bw.get("r", 0),
                    wickets=bw.get("w", 0),
                    dots=0,  # API may not provide dot balls
                    lbw_bowled=0,  # Need to parse from dismissal strings
                    overs_detail={},  # Not available from scorecard API
                )
                bowling[name] = entry

            # Parse fielding from dismissal strings
            for b in innings_data.get("batting", []):
                dismissal = b.get("dismissal-text", "") or b.get("dismissal", "")
                if not dismissal or dismissal == "not out":
                    continue
                bowler_name = None
                # Parse "c Fielder b Bowler" style strings
                self._parse_dismissal(dismissal, batting, bowling, fielding)

        teams = match_data.get("teams", [])
        status = "in_progress" if match_data.get("matchStarted") and not match_data.get("matchEnded") else \
                 "complete" if match_data.get("matchEnded") else "upcoming"

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

    def _parse_dismissal(self, text: str, batting, bowling, fielding):
        """Parse dismissal string like 'c Dhoni b Bumrah' into fielding credits."""
        import re
        text = text.strip()

        # "c FielderName b BowlerName"
        caught_match = re.match(r"c (.+?) b (.+)", text)
        if caught_match:
            fielder = caught_match.group(1).strip()
            if fielder not in fielding:
                fielding[fielder] = FieldingEntry(player=fielder)
            fielding[fielder].catches += 1
            # Count lbw/bowled for bowler
            return

        # "b BowlerName"
        if text.startswith("b "):
            bowler = text[2:].strip()
            if bowler in bowling:
                bowling[bowler].lbw_bowled += 1
            return

        # "lbw b BowlerName"
        if text.startswith("lbw b "):
            bowler = text[6:].strip()
            if bowler in bowling:
                bowling[bowler].lbw_bowled += 1
            return

        # "st FielderName b BowlerName"
        st_match = re.match(r"st (.+?) b (.+)", text)
        if st_match:
            fielder = st_match.group(1).strip()
            if fielder not in fielding:
                fielding[fielder] = FieldingEntry(player=fielder)
            fielding[fielder].stumpings += 1
            return

        # "run out (FielderName)"
        ro_match = re.match(r"run out \((.+?)\)", text)
        if ro_match:
            fielder = ro_match.group(1).strip()
            # Could have multiple fielders separated by /
            for f in fielder.split("/"):
                f = f.strip()
                if f:
                    if f not in fielding:
                        fielding[f] = FieldingEntry(player=f)
                    fielding[f].runouts += 1
