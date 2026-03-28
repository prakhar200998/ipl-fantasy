"""Adapter for local Cricsheet JSON files."""
import json
import glob
import os
from collections import defaultdict

from adapters.base import DataSourceAdapter
from models import MatchScorecard, BattingEntry, BowlingEntry, FieldingEntry

BOWLING_WICKET_KINDS = {"caught", "caught and bowled", "bowled", "lbw", "stumped", "hit wicket"}
LBW_BOWLED_KINDS = {"lbw", "bowled"}
DISMISSED_KINDS = {"caught", "caught and bowled", "bowled", "lbw", "stumped", "hit wicket", "run out"}


class CricsheetAdapter(DataSourceAdapter):
    def __init__(self, data_dir: str):
        self.data_dir = data_dir

    def get_match_list(self, season: str) -> list[dict]:
        """Get all IPL matches for a given season."""
        matches = []
        for filepath in sorted(glob.glob(os.path.join(self.data_dir, "*.json"))):
            fname = os.path.basename(filepath)
            if not fname[0].isdigit():
                continue
            try:
                with open(filepath) as f:
                    data = json.load(f)
            except (json.JSONDecodeError, IOError):
                continue
            info = data.get("info", {})
            if str(info.get("season")) != str(season):
                continue
            match_id = fname.replace(".json", "")
            teams = list(info.get("players", {}).keys())
            date = info.get("dates", [""])[0]
            matches.append({
                "match_id": match_id,
                "date": date,
                "teams": teams,
                "venue": info.get("city", ""),
                "status": "complete",
            })
        return matches

    def get_scorecard(self, match_id: str) -> MatchScorecard | None:
        """Parse a Cricsheet JSON file into a MatchScorecard."""
        filepath = os.path.join(self.data_dir, f"{match_id}.json")
        if not os.path.exists(filepath):
            return None
        with open(filepath) as f:
            data = json.load(f)

        info = data["info"]
        playing_xi = set()
        for team, players in info.get("players", {}).items():
            playing_xi.update(players)

        teams = list(info.get("players", {}).keys())
        date = info.get("dates", [""])[0]
        venue = info.get("city", "")

        batting = {}
        bowling = {}
        fielding = {}
        batters_who_batted = set()

        for innings in data.get("innings", []):
            team = innings["team"]
            for over_data in innings.get("overs", []):
                over_num = over_data["over"]
                for delivery in over_data.get("deliveries", []):
                    batter = delivery["batter"]
                    bowler = delivery["bowler"]
                    runs = delivery["runs"]
                    extras = delivery.get("extras", {})
                    batter_runs = runs["batter"]
                    is_wide = "wides" in extras
                    is_noball = "noballs" in extras

                    # Init entries if needed
                    if batter not in batting:
                        batting[batter] = BattingEntry(player=batter)
                    if bowler not in bowling:
                        bowling[bowler] = BowlingEntry(player=bowler)

                    # Batting
                    if not is_wide:
                        batting[batter].balls += 1
                        batters_who_batted.add(batter)
                    batting[batter].runs += batter_runs
                    if batter_runs > 0:
                        batters_who_batted.add(batter)
                    if batter_runs == 4:
                        batting[batter].fours += 1
                    elif batter_runs == 6:
                        batting[batter].sixes += 1

                    # Bowling
                    is_legal = not is_wide and not is_noball
                    bowling_runs = batter_runs + extras.get("wides", 0) + extras.get("noballs", 0)
                    bowling[bowler].runs += bowling_runs

                    over_key = f"{team}_{over_num}"
                    if over_key not in bowling[bowler].overs_detail:
                        bowling[bowler].overs_detail[over_key] = {"balls": 0, "runs": 0}
                    bowling[bowler].overs_detail[over_key]["runs"] += bowling_runs

                    if is_legal:
                        bowling[bowler].balls += 1
                        bowling[bowler].overs_detail[over_key]["balls"] += 1

                    if is_legal and runs["total"] == 0:
                        bowling[bowler].dots += 1

                    # Wickets
                    for wicket in delivery.get("wickets", []):
                        kind = wicket["kind"]
                        player_out = wicket["player_out"]

                        if player_out not in batting:
                            batting[player_out] = BattingEntry(player=player_out)
                        if kind in DISMISSED_KINDS:
                            batting[player_out].dismissed = True
                            batters_who_batted.add(player_out)
                        if kind in BOWLING_WICKET_KINDS:
                            bowling[bowler].wickets += 1
                            if kind in LBW_BOWLED_KINDS:
                                bowling[bowler].lbw_bowled += 1

                        # Fielding
                        fielders_list = wicket.get("fielders", [])
                        if kind == "caught" and fielders_list:
                            name = fielders_list[0]["name"]
                            if name not in fielding:
                                fielding[name] = FieldingEntry(player=name)
                            fielding[name].catches += 1
                        elif kind == "caught and bowled":
                            if bowler not in fielding:
                                fielding[bowler] = FieldingEntry(player=bowler)
                            fielding[bowler].catches += 1
                        elif kind == "stumped" and fielders_list:
                            name = fielders_list[0]["name"]
                            if name not in fielding:
                                fielding[name] = FieldingEntry(player=name)
                            fielding[name].stumpings += 1
                        elif kind == "run out" and fielders_list:
                            for fielder in fielders_list:
                                name = fielder["name"]
                                if name not in fielding:
                                    fielding[name] = FieldingEntry(player=name)
                                fielding[name].runouts += 1

        return MatchScorecard(
            match_id=match_id,
            date=date,
            teams=teams,
            venue=venue,
            status="complete",
            playing_xi=playing_xi,
            batting=batting,
            bowling=bowling,
            fielding=fielding,
            batters_who_batted=batters_who_batted,
        )
