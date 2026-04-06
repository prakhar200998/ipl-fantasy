"""Fantasy points calculator. Accepts a MatchScorecard and returns per-player points."""
from models import MatchScorecard

BOWLING_WICKET_KINDS = {"caught", "caught and bowled", "bowled", "lbw", "stumped", "hit wicket"}
LBW_BOWLED_KINDS = {"lbw", "bowled"}
DISMISSED_KINDS = {"caught", "caught and bowled", "bowled", "lbw", "stumped", "hit wicket", "run out"}


def calculate_fantasy_points(scorecard: MatchScorecard) -> dict[str, dict]:
    """Calculate fantasy points for all players in a match.

    Returns dict of player_name -> {
        "batting_pts": int,
        "bowling_pts": int,
        "fielding_pts": int,
        "total_pts": int,
        "breakdown": {...}
    }
    """
    player_points = {}

    for player in scorecard.playing_xi:
        batting_pts = 0
        bowling_pts = 0
        fielding_pts = 0
        breakdown = {"playing_xi": 4}
        base = 4  # Playing XI bonus

        # --- BATTING ---
        if player in scorecard.batters_who_batted:
            b = scorecard.batting[player]
            runs = b.runs
            balls = b.balls

            batting_pts += runs * 1
            batting_pts += b.fours * 4
            batting_pts += b.sixes * 6

            if b.dismissed and runs == 0:
                batting_pts -= 2

            # Milestone (highest only)
            if runs >= 100:
                batting_pts += 16
            elif runs >= 75:
                batting_pts += 12
            elif runs >= 50:
                batting_pts += 8
            elif runs >= 25:
                batting_pts += 4

            # Strike rate bonus (min 10 balls OR 20 runs)
            if balls >= 10 or runs >= 20:
                if balls > 0:
                    sr = (runs / balls) * 100
                    if sr > 190:
                        batting_pts += 8
                    elif sr > 170:
                        batting_pts += 6
                    elif sr > 150:
                        batting_pts += 4
                    elif sr >= 130:
                        batting_pts += 2
                    elif 70 <= sr <= 100:
                        batting_pts -= 2
                    elif 60 <= sr < 70:
                        batting_pts -= 4
                    elif 50 <= sr < 60:
                        batting_pts -= 6

            breakdown["batting"] = {
                "runs": runs, "balls": balls, "fours": b.fours,
                "sixes": b.sixes, "dismissed": b.dismissed,
            }

        # --- BOWLING ---
        if player in scorecard.bowling:
            bw = scorecard.bowling[player]

            bowling_pts += bw.dots * 2
            bowling_pts += bw.wickets * 30
            bowling_pts += bw.lbw_bowled * 8

            # Maiden overs
            maidens = 0
            for over_key, od in bw.overs_detail.items():
                if od["balls"] == 6 and od["runs"] == 0:
                    bowling_pts += 12
                    maidens += 1

            # Wicket milestone (highest only)
            w = bw.wickets
            if w >= 5:
                bowling_pts += 16
            elif w >= 4:
                bowling_pts += 12
            elif w >= 3:
                bowling_pts += 8

            # Economy rate bonus (min 2 overs = 12 balls)
            if bw.balls >= 12:
                overs = bw.balls / 6
                economy = bw.runs / overs
                if economy < 5:
                    bowling_pts += 8
                elif economy < 6:
                    bowling_pts += 6
                elif economy <= 7:
                    bowling_pts += 4
                elif economy <= 8:
                    bowling_pts += 2
                elif 10 <= economy <= 11:
                    bowling_pts -= 2
                elif 11 < economy <= 12:
                    bowling_pts -= 4
                elif economy > 12:
                    bowling_pts -= 6

            breakdown["bowling"] = {
                "balls": bw.balls, "runs": bw.runs, "wickets": bw.wickets,
                "dots": bw.dots, "maidens": maidens, "lbw_bowled": bw.lbw_bowled,
            }

        # --- FIELDING ---
        if player in scorecard.fielding:
            fl = scorecard.fielding[player]
            fielding_pts += fl.catches * 8
            if fl.catches >= 3:
                fielding_pts += 4
            fielding_pts += fl.direct_runouts * 10
            fielding_pts += fl.assisted_runouts * 5
            fielding_pts += fl.stumpings * 12

            breakdown["fielding"] = {
                "catches": fl.catches, "direct_runouts": fl.direct_runouts,
                "assisted_runouts": fl.assisted_runouts, "stumpings": fl.stumpings,
            }

        total = base + batting_pts + bowling_pts + fielding_pts
        player_points[player] = {
            "batting_pts": batting_pts,
            "bowling_pts": bowling_pts,
            "fielding_pts": fielding_pts,
            "total_pts": total,
            "breakdown": breakdown,
        }

    return player_points
