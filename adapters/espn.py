"""ESPN API adapter — free, unauthenticated source for bowling dot balls.

Endpoint: https://site.web.api.espn.com/apis/site/v2/sports/cricket/8048/summary?event={id}
League 8048 = IPL. No API key needed.
"""
import logging
import httpx
from name_mapping import get_display_name
from models import MatchScorecard

logger = logging.getLogger(__name__)

ESPN_BASE = "https://site.web.api.espn.com/apis/site/v2/sports/cricket"
IPL_LEAGUE_ID = "8048"


def fetch_espn_match_data(espn_event_id: str) -> dict:
    """Fetch ESPN summary and return {dots: {name: count}, playing_xi: set(names)}.

    Single API call provides both bowling dots and the full playing XI roster
    (including impact subs who didn't bat/bowl/field).
    """
    url = f"{ESPN_BASE}/{IPL_LEAGUE_ID}/summary?event={espn_event_id}"
    try:
        resp = httpx.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("ESPN API error for event %s: %s", espn_event_id, e)
        return {"dots": {}, "playing_xi": set()}

    dots: dict[str, int] = {}
    playing_xi: set[str] = set()

    for team in data.get("rosters", []):
        for player in team.get("roster", []):
            name = player.get("athlete", {}).get("displayName", "")
            if not name:
                continue
            display = get_display_name(name)
            playing_xi.add(display)
            for ls in player.get("linescores", []):
                for cat in ls.get("statistics", {}).get("categories", []):
                    stat_dict = {s["name"]: s["value"] for s in cat.get("stats", [])}
                    if stat_dict.get("overs", 0) > 0 and "dots" in stat_dict:
                        dots[display] = dots.get(display, 0) + int(stat_dict["dots"])

    return {"dots": dots, "playing_xi": playing_xi}


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
    espn_id = find_espn_event_id(date, teams) if date else None
    if not espn_id:
        logger.info("Could not find ESPN event for enrichment (date=%s)", date)
        return

    espn_data = fetch_espn_match_data(espn_id)

    # Enrich playing XI (always — ESPN has impact subs CricketData misses)
    if espn_data["playing_xi"]:
        added = espn_data["playing_xi"] - scorecard.playing_xi
        scorecard.playing_xi |= espn_data["playing_xi"]
        if added:
            logger.info("ESPN added %d players to playing XI: %s", len(added), added)

    # Enrich bowling dots (skip if already populated)
    total_existing = sum(e.dots for e in scorecard.bowling.values())
    if total_existing > 0:
        return

    dots_map = espn_data["dots"]
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


# Backward-compatible alias
enrich_bowling_dots = enrich_from_espn
