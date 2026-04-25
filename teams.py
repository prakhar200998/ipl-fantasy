"""Active fantasy team rosters — Phase 2 (post mid-season auction).

Re-exports TEAMS_PHASE2 as TEAMS so existing call sites work unchanged.
For matches dated < PHASE2_CUTOFF_DATE, callers should pass the match date
to get_captain_vc() to retrieve the Phase 1 mapping.
"""

from teams_phase1 import TEAMS_PHASE1, get_captain_vc_phase1
from teams_phase2 import TEAMS_PHASE2
from config import PHASE2_CUTOFF_DATE


TEAMS = TEAMS_PHASE2


def _captain_vc_for(teams_dict: dict) -> dict:
    result = {}
    for team_data in teams_dict.values():
        for p in team_data["players"]:
            if p.get("captain"):
                result[p["name"]] = "C"
            elif p.get("vice_captain"):
                result[p["name"]] = "VC"
    return result


_PHASE1_CVC = get_captain_vc_phase1()
_PHASE2_CVC = _captain_vc_for(TEAMS_PHASE2)


def get_captain_vc(match_date: str | None = None) -> dict:
    """Return C/VC map for the relevant phase.

    match_date: 'YYYY-MM-DD'. If < PHASE2_CUTOFF_DATE → Phase 1 map; else
    Phase 2 (default). Callers that don't know the date (e.g., live ingest
    of new matches) get Phase 2.
    """
    if match_date and match_date[:10] < PHASE2_CUTOFF_DATE:
        return _PHASE1_CVC
    return _PHASE2_CVC


def get_player_names() -> list[str]:
    """All Phase 2 player names (active squad)."""
    return [p["name"] for td in TEAMS.values() for p in td["players"]]


def get_player_meta() -> dict:
    """Phase 2 player metadata: player_name -> {role, ipl_team, designation, fantasy_team}."""
    result = {}
    for team_name, team_data in TEAMS.items():
        for p in team_data["players"]:
            result[p["name"]] = {
                "role": p.get("role", ""),
                "ipl_team": p.get("ipl_team", ""),
                "designation": "C" if p.get("captain") else "VC" if p.get("vice_captain") else "",
                "fantasy_team": team_name,
            }
    return result
