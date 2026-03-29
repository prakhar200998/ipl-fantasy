"""ESPN API adapter — free, unauthenticated source for bowling dot balls.

Endpoint: https://site.web.api.espn.com/apis/site/v2/sports/cricket/8048/summary?event={id}
League 8048 = IPL. No API key needed.
"""
import logging
import httpx
from name_mapping import get_display_name

logger = logging.getLogger(__name__)

ESPN_BASE = "https://site.web.api.espn.com/apis/site/v2/sports/cricket"
IPL_LEAGUE_ID = "8048"


def fetch_espn_bowling_dots(espn_event_id: str) -> dict[str, int]:
    """Return {display_name: dot_count} for every bowler in the match."""
    url = f"{ESPN_BASE}/{IPL_LEAGUE_ID}/summary?event={espn_event_id}"
    try:
        resp = httpx.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("ESPN API error for event %s: %s", espn_event_id, e)
        return {}

    dots: dict[str, int] = {}
    for team in data.get("rosters", []):
        for player in team.get("roster", []):
            name = player.get("athlete", {}).get("displayName", "")
            if not name:
                continue
            for ls in player.get("linescores", []):
                for cat in ls.get("statistics", {}).get("categories", []):
                    stat_dict = {s["name"]: s["value"] for s in cat.get("stats", [])}
                    if stat_dict.get("overs", 0) > 0 and "dots" in stat_dict:
                        display = get_display_name(name)
                        dots[display] = dots.get(display, 0) + int(stat_dict["dots"])
    return dots


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
