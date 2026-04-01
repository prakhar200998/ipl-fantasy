"""ESPN API adapter — free, unauthenticated source for match data.

Endpoint: https://site.web.api.espn.com/apis/site/v2/sports/cricket/8048/summary?event={id}
League 8048 = IPL. No API key needed.

Provides: full scorecard (batting, bowling with dots, fielding catches/stumpings,
playing XI including impact subs). Missing: lbw/bowled bonus, runout credits
(these get filled in by CricketData scorecard at match completion).
"""
import logging
import httpx
from name_mapping import get_display_name
from models import MatchScorecard, BattingEntry, BowlingEntry, FieldingEntry

logger = logging.getLogger(__name__)

ESPN_BASE = "https://site.web.api.espn.com/apis/site/v2/sports/cricket"
IPL_LEAGUE_ID = "8048"


def _fetch_espn_summary(espn_event_id: str) -> dict | None:
    """Fetch raw ESPN summary JSON for an event."""
    url = f"{ESPN_BASE}/{IPL_LEAGUE_ID}/summary?event={espn_event_id}"
    try:
        resp = httpx.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error("ESPN API error for event %s: %s", espn_event_id, e)
        return None


def _overs_to_balls(overs) -> int:
    """Convert overs (float like 3.2) to total legal deliveries."""
    try:
        full = int(overs)
        partial = round((float(overs) - full) * 10)
        return full * 6 + partial
    except (ValueError, TypeError):
        return 0


def get_espn_scorecard(date: str, teams: list[str]) -> MatchScorecard | None:
    """Build a full MatchScorecard from ESPN's free API.

    Has: playing XI, batting, bowling (with dots!), catches, stumpings.
    Missing: lbw/bowled bonus, runout credits (filled by CricketData at match end).
    """
    espn_id = find_espn_event_id(date, teams) if date else None
    if not espn_id:
        logger.info("Could not find ESPN event (date=%s)", date)
        return None

    data = _fetch_espn_summary(espn_id)
    if not data:
        return None

    playing_xi: set[str] = set()
    batting: dict[str, BattingEntry] = {}
    bowling: dict[str, BowlingEntry] = {}
    fielding: dict[str, FieldingEntry] = {}
    batters_who_batted: set[str] = set()

    for team in data.get("rosters", []):
        for player in team.get("roster", []):
            raw_name = player.get("athlete", {}).get("displayName", "")
            if not raw_name:
                continue
            name = get_display_name(raw_name)
            playing_xi.add(name)

            for ls in player.get("linescores", []):
                for cat in ls.get("statistics", {}).get("categories", []):
                    s = {st["name"]: st["value"] for st in cat.get("stats", [])}

                    # --- Batting ---
                    if s.get("batted", 0) > 0:
                        runs = int(s.get("runs", 0))
                        balls = int(s.get("ballsFaced", 0))
                        fours = int(s.get("fours", 0))
                        sixes = int(s.get("sixes", 0))
                        dismissed = int(s.get("outs", 0)) > 0

                        if name in batting:
                            e = batting[name]
                            e.runs += runs
                            e.balls += balls
                            e.fours += fours
                            e.sixes += sixes
                            e.dismissed = e.dismissed or dismissed
                        else:
                            batting[name] = BattingEntry(
                                player=name, runs=runs, balls=balls,
                                fours=fours, sixes=sixes, dismissed=dismissed,
                            )
                        if balls > 0 or runs > 0 or dismissed:
                            batters_who_batted.add(name)

                    # --- Bowling ---
                    if s.get("overs", 0) > 0:
                        total_balls = _overs_to_balls(s.get("overs", 0))
                        runs_conceded = int(s.get("conceded", 0))
                        wickets = int(s.get("wickets", 0))
                        dots = int(s.get("dots", 0))
                        maidens = int(s.get("maidens", 0))

                        overs_detail: dict = {}
                        for mi in range(maidens):
                            overs_detail[f"maiden_{name}_{mi}"] = {"balls": 6, "runs": 0}

                        if name in bowling:
                            e = bowling[name]
                            e.balls += total_balls
                            e.runs += runs_conceded
                            e.wickets += wickets
                            e.dots += dots
                            e.overs_detail.update(overs_detail)
                        else:
                            bowling[name] = BowlingEntry(
                                player=name, balls=total_balls, runs=runs_conceded,
                                wickets=wickets, dots=dots, lbw_bowled=0,
                                overs_detail=overs_detail,
                            )

                    # --- Fielding (catches + stumpings only; runouts need CricketData) ---
                    catches = int(s.get("caught", 0))
                    stumpings = int(s.get("stumped", 0))
                    if catches > 0 or stumpings > 0:
                        if name in fielding:
                            fielding[name].catches += catches
                            fielding[name].stumpings += stumpings
                        else:
                            fielding[name] = FieldingEntry(
                                player=name, catches=catches, stumpings=stumpings,
                            )

    # Determine match status from ESPN header
    status = "in_progress"
    try:
        comp = data.get("header", {}).get("competitions", [{}])[0]
        state = comp.get("status", {}).get("type", {}).get("description", "")
        if state.lower() in ("result", "complete"):
            status = "complete"
        elif state.lower() in ("abandoned", "no result"):
            status = "abandoned"
    except (IndexError, KeyError):
        pass

    return MatchScorecard(
        match_id="",  # caller sets this
        date=date,
        teams=teams,
        venue="",
        status=status,
        playing_xi=playing_xi,
        batting=batting,
        bowling=bowling,
        fielding=fielding,
        batters_who_batted=batters_who_batted,
    )


def discover_espn_matches(start_date: str, end_date: str) -> list[dict]:
    """Discover IPL matches from ESPN scoreboard by scanning a date range.

    Free, no auth. Returns list of dicts with: espn_id, date, teams, status, name.
    """
    from datetime import datetime, timedelta
    results = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end:
        date_compact = current.strftime("%Y%m%d")
        try:
            resp = httpx.get(
                f"{ESPN_BASE}/{IPL_LEAGUE_ID}/scoreboard?dates={date_compact}",
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("ESPN discovery error for %s: %s", date_compact, e)
            current += timedelta(days=1)
            continue

        for event in data.get("events", []):
            teams = []
            for comp in event.get("competitions", []):
                for c in comp.get("competitors", []):
                    teams.append(c.get("team", {}).get("displayName", ""))
            status_desc = event.get("status", {}).get("type", {}).get("description", "")
            if status_desc.lower() in ("result", "complete"):
                status = "complete"
            elif status_desc.lower() in ("abandoned", "no result"):
                status = "abandoned"
            elif status_desc.lower() == "live":
                status = "in_progress"
            else:
                status = "upcoming"
            results.append({
                "espn_id": event.get("id", ""),
                "date": current.strftime("%Y-%m-%d"),
                "teams": teams,
                "status": status,
                "name": event.get("name", ""),
            })

        current += timedelta(days=1)

    return results


def find_espn_event_id(date: str, teams: list[str]) -> str | None:
    """Find the ESPN event ID for an IPL match by date.

    date: 'YYYY-MM-DD'
    teams: list of team names (from Cricbuzz)
    Returns ESPN event ID string or None.
    """
    date_compact = date.replace("-", "")
    url = f"{ESPN_BASE}/{IPL_LEAGUE_ID}/scoreboard?dates={date_compact}"
    try:
        resp = httpx.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("ESPN scoreboard error for %s: %s", date, e)
        return None

    events = data.get("events", [])
    if len(events) == 1:
        return events[0].get("id")

    # Multiple matches on same day — match by team names
    teams_lower = {t.lower() for t in teams}
    for event in events:
        event_teams = set()
        for comp in event.get("competitions", []):
            for c in comp.get("competitors", []):
                event_teams.add(c.get("team", {}).get("displayName", "").lower())
        if teams_lower & event_teams:
            return event.get("id")

    # Fallback: return first event
    if events:
        return events[0].get("id")
    return None


def enrich_from_espn(
    scorecard: MatchScorecard, date: str, teams: list[str],
) -> None:
    """Enrich scorecard with ESPN data: bowling dots + full playing XI.

    ESPN's roster includes impact subs who didn't bat/bowl/field,
    ensuring they get their 4-point playing XI bonus.
    No-op for dots if already populated (e.g. from Cricsheet re-scoring).
    """
    espn_sc = get_espn_scorecard(date, teams)
    if not espn_sc:
        return

    # Enrich playing XI (always — ESPN has impact subs CricketData misses)
    if espn_sc.playing_xi:
        added = espn_sc.playing_xi - scorecard.playing_xi
        scorecard.playing_xi |= espn_sc.playing_xi
        if added:
            logger.info("ESPN added %d players to playing XI: %s", len(added), added)

    # Enrich bowling dots (skip if already populated)
    total_existing = sum(e.dots for e in scorecard.bowling.values())
    if total_existing > 0:
        return

    enriched = 0
    for name, entry in scorecard.bowling.items():
        espn_bowl = espn_sc.bowling.get(name)
        if espn_bowl and espn_bowl.dots > 0:
            entry.dots = espn_bowl.dots
            enriched += 1

    if enriched:
        logger.info(
            "Enriched %d/%d bowlers with ESPN dots",
            enriched, len(scorecard.bowling),
        )


# Backward-compatible alias
enrich_bowling_dots = enrich_from_espn
