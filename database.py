"""SQLite database for fantasy points storage."""
import sqlite3
import json
import os
from config import DB_PATH


def get_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            date TEXT,
            teams_json TEXT,
            venue TEXT,
            status TEXT DEFAULT 'upcoming',
            last_updated TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS player_match_points (
            match_id TEXT,
            player_name TEXT,
            batting_pts INTEGER DEFAULT 0,
            bowling_pts INTEGER DEFAULT 0,
            fielding_pts INTEGER DEFAULT 0,
            raw_pts INTEGER DEFAULT 0,
            total_pts INTEGER DEFAULT 0,
            breakdown_json TEXT,
            PRIMARY KEY (match_id, player_name),
            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        );

        CREATE TABLE IF NOT EXISTS teams (
            team_id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS roster (
            team_id INTEGER,
            player_name TEXT,
            role TEXT,
            ipl_team TEXT,
            designation TEXT DEFAULT '',
            added_date TEXT DEFAULT '2026-01-01',
            removed_date TEXT,
            FOREIGN KEY (team_id) REFERENCES teams(team_id)
        );

        CREATE INDEX IF NOT EXISTS idx_pmp_player ON player_match_points(player_name);
        CREATE INDEX IF NOT EXISTS idx_pmp_match ON player_match_points(match_id);
        CREATE INDEX IF NOT EXISTS idx_roster_team ON roster(team_id);
    """)
    conn.commit()
    conn.close()


def seed_teams(teams_dict: dict):
    """Seed teams and rosters from the TEAMS dict (new format with player metadata)."""
    conn = get_db()
    for team_name, team_data in teams_dict.items():
        conn.execute(
            "INSERT OR IGNORE INTO teams (team_name) VALUES (?)",
            (team_name,)
        )
        team_id = conn.execute(
            "SELECT team_id FROM teams WHERE team_name = ?", (team_name,)
        ).fetchone()["team_id"]
        players = team_data["players"]
        for p in players:
            player_name = p["name"]
            role = p.get("role", "")
            ipl_team = p.get("ipl_team", "")
            designation = "C" if p.get("captain") else "VC" if p.get("vice_captain") else ""
            existing = conn.execute(
                "SELECT 1 FROM roster WHERE team_id = ? AND player_name = ? AND removed_date IS NULL",
                (team_id, player_name)
            ).fetchone()
            if not existing:
                conn.execute(
                    "INSERT INTO roster (team_id, player_name, role, ipl_team, designation) VALUES (?, ?, ?, ?, ?)",
                    (team_id, player_name, role, ipl_team, designation)
                )
    conn.commit()
    conn.close()


def upsert_match(match_id: str, date: str, teams: list, venue: str, status: str):
    conn = get_db()
    conn.execute("""
        INSERT INTO matches (match_id, date, teams_json, venue, status, last_updated)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(match_id) DO UPDATE SET
            status = excluded.status,
            last_updated = datetime('now')
    """, (match_id, date, json.dumps(teams), venue, status))
    conn.commit()
    conn.close()


def upsert_player_points(match_id: str, player_name: str, pts: dict,
                         captain_vc: dict[str, str] | None = None):
    if captain_vc is None:
        captain_vc = {}
    raw_pts = pts["total_pts"]
    designation = captain_vc.get(player_name, "")
    if designation == "C":
        total_pts = raw_pts * 2
    elif designation == "VC":
        total_pts = int(raw_pts * 1.5)
    else:
        total_pts = raw_pts
    conn = get_db()
    conn.execute("""
        INSERT INTO player_match_points (match_id, player_name, batting_pts, bowling_pts, fielding_pts, raw_pts, total_pts, breakdown_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(match_id, player_name) DO UPDATE SET
            batting_pts = excluded.batting_pts,
            bowling_pts = excluded.bowling_pts,
            fielding_pts = excluded.fielding_pts,
            raw_pts = excluded.raw_pts,
            total_pts = excluded.total_pts,
            breakdown_json = excluded.breakdown_json
    """, (match_id, player_name, pts["batting_pts"], pts["bowling_pts"],
          pts["fielding_pts"], raw_pts, total_pts, json.dumps(pts["breakdown"])))
    conn.commit()
    conn.close()


def bulk_upsert_player_points(match_id: str, all_points: dict[str, dict],
                              captain_vc: dict[str, str] | None = None):
    """Upsert all player points for a match in a single transaction.

    captain_vc: optional dict of player_name -> 'C' or 'VC'.
    Captain gets 2x total_pts, Vice Captain gets 1.5x. raw_pts stores the original.
    """
    if captain_vc is None:
        captain_vc = {}
    conn = get_db()
    for player_name, pts in all_points.items():
        raw_pts = pts["total_pts"]
        designation = captain_vc.get(player_name, "")
        if designation == "C":
            total_pts = raw_pts * 2
        elif designation == "VC":
            total_pts = int(raw_pts * 1.5)
        else:
            total_pts = raw_pts
        conn.execute("""
            INSERT INTO player_match_points (match_id, player_name, batting_pts, bowling_pts, fielding_pts, raw_pts, total_pts, breakdown_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(match_id, player_name) DO UPDATE SET
                batting_pts = excluded.batting_pts,
                bowling_pts = excluded.bowling_pts,
                fielding_pts = excluded.fielding_pts,
                raw_pts = excluded.raw_pts,
                total_pts = excluded.total_pts,
                breakdown_json = excluded.breakdown_json
        """, (match_id, player_name, pts["batting_pts"], pts["bowling_pts"],
              pts["fielding_pts"], raw_pts, total_pts, json.dumps(pts["breakdown"])))
    conn.commit()
    conn.close()


def wipe_match_data():
    """Delete all match and player_match_points data (keeps teams/roster)."""
    conn = get_db()
    conn.execute("DELETE FROM player_match_points")
    conn.execute("DELETE FROM matches")
    conn.commit()
    conn.close()


def wipe_all():
    """Delete ALL data from all tables — full reset."""
    conn = get_db()
    conn.execute("DELETE FROM player_match_points")
    conn.execute("DELETE FROM matches")
    conn.execute("DELETE FROM roster")
    conn.execute("DELETE FROM teams")
    conn.commit()
    conn.close()


def get_standings() -> list[dict]:
    """Get team standings with top-11 scoring."""
    conn = get_db()

    teams = conn.execute("SELECT team_id, team_name FROM teams ORDER BY team_name").fetchall()
    standings = []

    for team in teams:
        # Get all active roster players and their total points
        players = conn.execute("""
            SELECT r.player_name,
                   r.role,
                   r.ipl_team,
                   r.designation,
                   COALESCE(SUM(p.total_pts), 0) as total_pts,
                   COUNT(p.match_id) as matches_played
            FROM roster r
            LEFT JOIN player_match_points p ON r.player_name = p.player_name
            WHERE r.team_id = ? AND r.removed_date IS NULL
            GROUP BY r.player_name
            ORDER BY total_pts DESC
        """, (team["team_id"],)).fetchall()

        player_list = [dict(p) for p in players]
        # Top 11 scoring
        top11 = player_list[:11]
        top11_total = sum(p["total_pts"] for p in top11)

        standings.append({
            "team_name": team["team_name"],
            "team_id": team["team_id"],
            "total_pts": top11_total,
            "players": player_list,
            "top11": top11,
        })

    standings.sort(key=lambda x: x["total_pts"], reverse=True)

    # --- rank_change: compare current rank vs rank without latest match ---
    matches = conn.execute(
        "SELECT match_id FROM matches ORDER BY date ASC, match_id ASC"
    ).fetchall()

    if len(matches) >= 2:
        latest_mid = matches[-1]["match_id"]

        # Compute previous standings (excluding latest match)
        prev_standings = []
        for team in teams:
            prev_players = conn.execute("""
                SELECT r.player_name,
                       COALESCE(SUM(p.total_pts), 0) as total_pts
                FROM roster r
                LEFT JOIN player_match_points p
                    ON r.player_name = p.player_name AND p.match_id != ?
                WHERE r.team_id = ? AND r.removed_date IS NULL
                GROUP BY r.player_name
                ORDER BY total_pts DESC
            """, (latest_mid, team["team_id"])).fetchall()
            prev_top11_total = sum(p["total_pts"] for p in prev_players[:11])
            prev_standings.append({
                "team_id": team["team_id"],
                "total_pts": prev_top11_total,
            })

        prev_standings.sort(key=lambda x: x["total_pts"], reverse=True)
        prev_rank = {s["team_id"]: i + 1 for i, s in enumerate(prev_standings)}

        for i, team_data in enumerate(standings):
            current_rank = i + 1
            old_rank = prev_rank.get(team_data["team_id"], current_rank)
            team_data["rank_change"] = old_rank - current_rank
    else:
        for team_data in standings:
            team_data["rank_change"] = None

    # --- pts_history: per-match point totals for each team's current top-11 ---
    match_ids = [m["match_id"] for m in matches]

    for team_data in standings:
        top11_names = [p["player_name"] for p in team_data["top11"]]

        if top11_names and match_ids:
            placeholders_names = ",".join("?" * len(top11_names))
            placeholders_matches = ",".join("?" * len(match_ids))

            rows = conn.execute(f"""
                SELECT match_id, SUM(total_pts) as match_total
                FROM player_match_points
                WHERE player_name IN ({placeholders_names})
                  AND match_id IN ({placeholders_matches})
                GROUP BY match_id
            """, top11_names + match_ids).fetchall()

            match_totals = {r["match_id"]: r["match_total"] for r in rows}
            team_data["pts_history"] = [match_totals.get(mid, 0) for mid in match_ids]
        else:
            team_data["pts_history"] = []

    conn.close()
    return standings


def get_team_detail(team_name: str) -> dict | None:
    conn = get_db()
    team = conn.execute(
        "SELECT team_id, team_name FROM teams WHERE team_name = ?", (team_name,)
    ).fetchone()
    if not team:
        conn.close()
        return None

    # Get aggregated player stats
    players = conn.execute("""
        SELECT r.player_name,
               r.role,
               r.ipl_team,
               r.designation,
               COALESCE(SUM(p.total_pts), 0) as total_pts,
               COALESCE(SUM(p.batting_pts), 0) as batting_pts,
               COALESCE(SUM(p.bowling_pts), 0) as bowling_pts,
               COALESCE(SUM(p.fielding_pts), 0) as fielding_pts,
               COUNT(p.match_id) as matches_played
        FROM roster r
        LEFT JOIN player_match_points p ON r.player_name = p.player_name
        WHERE r.team_id = ? AND r.removed_date IS NULL
        GROUP BY r.player_name
        ORDER BY total_pts DESC
    """, (team["team_id"],)).fetchall()

    # Get all matches in chronological order
    matches = conn.execute("""
        SELECT match_id, date, teams_json, status
        FROM matches
        ORDER BY date ASC, match_id ASC
    """).fetchall()

    match_list = []
    for i, m in enumerate(matches):
        match_list.append({
            "match_id": m["match_id"],
            "label": f"M{i + 1}",
            "date": m["date"],
            "teams": json.loads(m["teams_json"]),
        })

    # Get per-match scores for roster players
    roster_names = [p["player_name"] for p in players]
    per_match = {}
    if roster_names:
        placeholders = ",".join("?" * len(roster_names))
        rows = conn.execute(f"""
            SELECT player_name, match_id, total_pts
            FROM player_match_points
            WHERE player_name IN ({placeholders})
        """, roster_names).fetchall()
        for r in rows:
            per_match.setdefault(r["player_name"], {})[r["match_id"]] = r["total_pts"]

    # Build match_id -> label map
    mid_to_label = {m["match_id"]: f"M{i+1}" for i, m in enumerate(matches)}

    player_list = []
    for p in players:
        pd = dict(p)
        # Add per-match scores
        scores = per_match.get(p["player_name"], {})
        pd["match_scores"] = [
            {"label": mid_to_label[mid], "pts": pts}
            for mid, pts in scores.items()
            if mid in mid_to_label
        ]
        player_list.append(pd)

    top11 = player_list[:11]
    top11_total = sum(p["total_pts"] for p in top11)

    conn.close()
    return {
        "team_name": team["team_name"],
        "total_pts": top11_total,
        "players": player_list,
        "matches": match_list,
    }


def get_live_match_points(match_id: str) -> list[dict]:
    """Get all player points for a specific match."""
    conn = get_db()
    rows = conn.execute("""
        SELECT player_name, batting_pts, bowling_pts, fielding_pts, total_pts, breakdown_json
        FROM player_match_points
        WHERE match_id = ?
        ORDER BY total_pts DESC
    """, (match_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_latest_match() -> dict | None:
    conn = get_db()
    row = conn.execute("""
        SELECT match_id, date, teams_json, venue, status
        FROM matches
        ORDER BY date DESC, match_id DESC
        LIMIT 1
    """).fetchone()
    conn.close()
    if row:
        r = dict(row)
        r["teams"] = json.loads(r["teams_json"])
        return r
    return None


def get_match_count() -> int:
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) as cnt FROM matches").fetchone()
    conn.close()
    return row["cnt"]


def get_awards() -> dict:
    """Compute all awards/stats for the awards page."""
    conn = get_db()

    # Check if any matches exist
    match_count = conn.execute("SELECT COUNT(*) as cnt FROM matches").fetchone()["cnt"]
    if match_count == 0:
        conn.close()
        return {
            "best_xi_week": [],
            "top_batter_season": None,
            "top_bowler_season": None,
            "top_fielder_season": None,
            "top_batter_week": None,
            "top_bowler_week": None,
            "top_fielder_week": None,
            "best_match_performance": None,
            "biggest_dud": None,
            "most_consistent": None,
            "carry_award": None,
            "bench_burden": None,
        }

    # Get the most recent match_id
    latest = conn.execute("""
        SELECT match_id FROM matches ORDER BY date DESC, match_id DESC LIMIT 1
    """).fetchone()
    latest_match_id = latest["match_id"]

    # --- a. best_xi_week: Top 11 scorers in most recent match across all teams ---
    best_xi_week = [dict(r) for r in conn.execute("""
        SELECT p.player_name, p.total_pts,
               COALESCE(t.team_name, '') as team_name
        FROM player_match_points p
        LEFT JOIN roster r ON p.player_name = r.player_name AND r.removed_date IS NULL
        LEFT JOIN teams t ON r.team_id = t.team_id
        WHERE p.match_id = ?
        ORDER BY p.total_pts DESC
        LIMIT 11
    """, (latest_match_id,)).fetchall()]

    # --- b. top_batter_season ---
    top_batter_season = _fetch_top_category(conn, "batting_pts")

    # --- c. top_bowler_season ---
    top_bowler_season = _fetch_top_category(conn, "bowling_pts")

    # --- d. top_fielder_season ---
    top_fielder_season = _fetch_top_category(conn, "fielding_pts")

    # --- e. top_batter_week ---
    top_batter_week = _fetch_top_category_match(conn, "batting_pts", latest_match_id)

    # --- f. top_bowler_week ---
    top_bowler_week = _fetch_top_category_match(conn, "bowling_pts", latest_match_id)

    # --- g. top_fielder_week ---
    top_fielder_week = _fetch_top_category_match(conn, "fielding_pts", latest_match_id)

    # --- h. best_match_performance: single highest total_pts in any match ---
    row = conn.execute("""
        SELECT p.player_name, p.total_pts, p.match_id,
               m.date as match_date, m.teams_json as match_teams
        FROM player_match_points p
        JOIN matches m ON p.match_id = m.match_id
        ORDER BY p.total_pts DESC
        LIMIT 1
    """).fetchone()
    best_match_performance = dict(row) if row else None
    if best_match_performance:
        best_match_performance["match_teams"] = json.loads(best_match_performance["match_teams"])

    # --- i. biggest_dud: lowest total_pts in any match ---
    row = conn.execute("""
        SELECT p.player_name, p.total_pts, p.match_id,
               m.date as match_date, m.teams_json as match_teams
        FROM player_match_points p
        JOIN matches m ON p.match_id = m.match_id
        ORDER BY p.total_pts ASC
        LIMIT 1
    """).fetchone()
    biggest_dud = dict(row) if row else None
    if biggest_dud:
        biggest_dud["match_teams"] = json.loads(biggest_dud["match_teams"])

    # --- j. most_consistent: highest avg pts/match, min 3 matches ---
    row = conn.execute("""
        SELECT p.player_name,
               ROUND(AVG(p.total_pts), 2) as avg_pts,
               COUNT(p.match_id) as matches_played,
               COALESCE(t.team_name, '') as team_name
        FROM player_match_points p
        LEFT JOIN roster r ON p.player_name = r.player_name AND r.removed_date IS NULL
        LEFT JOIN teams t ON r.team_id = t.team_id
        GROUP BY p.player_name
        HAVING COUNT(p.match_id) >= 3
        ORDER BY avg_pts DESC
        LIMIT 1
    """).fetchone()
    most_consistent = dict(row) if row else None

    # --- k. carry_award: player with highest % of their team's top-11 total ---
    carry_award = _compute_carry_award(conn)

    # --- l. bench_burden: best player NOT in any team's top 11 ---
    bench_burden = _compute_bench_burden(conn)

    conn.close()

    return {
        "best_xi_week": best_xi_week,
        "top_batter_season": top_batter_season,
        "top_bowler_season": top_bowler_season,
        "top_fielder_season": top_fielder_season,
        "top_batter_week": top_batter_week,
        "top_bowler_week": top_bowler_week,
        "top_fielder_week": top_fielder_week,
        "best_match_performance": best_match_performance,
        "biggest_dud": biggest_dud,
        "most_consistent": most_consistent,
        "carry_award": carry_award,
        "bench_burden": bench_burden,
    }


def _fetch_top_category(conn, pts_column: str) -> dict | None:
    """Top player by a cumulative points category (season-wide)."""
    row = conn.execute(f"""
        SELECT p.player_name,
               SUM(p.{pts_column}) as {pts_column},
               COALESCE(t.team_name, '') as team_name
        FROM player_match_points p
        LEFT JOIN roster r ON p.player_name = r.player_name AND r.removed_date IS NULL
        LEFT JOIN teams t ON r.team_id = t.team_id
        GROUP BY p.player_name
        ORDER BY {pts_column} DESC
        LIMIT 1
    """).fetchone()
    return dict(row) if row else None


def _fetch_top_category_match(conn, pts_column: str, match_id: str) -> dict | None:
    """Top player by a points category for a specific match."""
    row = conn.execute(f"""
        SELECT p.player_name,
               p.{pts_column},
               COALESCE(t.team_name, '') as team_name
        FROM player_match_points p
        LEFT JOIN roster r ON p.player_name = r.player_name AND r.removed_date IS NULL
        LEFT JOIN teams t ON r.team_id = t.team_id
        WHERE p.match_id = ?
        ORDER BY p.{pts_column} DESC
        LIMIT 1
    """, (match_id,)).fetchone()
    return dict(row) if row else None


def _compute_carry_award(conn) -> dict | None:
    """Player contributing highest % of their team's top-11 total."""
    teams = conn.execute("SELECT team_id, team_name FROM teams").fetchall()
    best = None

    for team in teams:
        players = conn.execute("""
            SELECT r.player_name,
                   COALESCE(SUM(p.total_pts), 0) as total_pts
            FROM roster r
            LEFT JOIN player_match_points p ON r.player_name = p.player_name
            WHERE r.team_id = ? AND r.removed_date IS NULL
            GROUP BY r.player_name
            ORDER BY total_pts DESC
        """, (team["team_id"],)).fetchall()

        top11 = players[:11]
        team_total = sum(p["total_pts"] for p in top11)
        if team_total <= 0:
            continue

        # The top player in the top 11 has the highest individual contribution
        top_player = top11[0]
        pct = round((top_player["total_pts"] / team_total) * 100, 2)

        if best is None or pct > best["percentage"]:
            best = {
                "player_name": top_player["player_name"],
                "total_pts": top_player["total_pts"],
                "team_total": team_total,
                "percentage": pct,
                "team_name": team["team_name"],
            }

    return best


def _compute_bench_burden(conn) -> dict | None:
    """Best player (by total_pts) NOT in any team's top 11."""
    teams = conn.execute("SELECT team_id, team_name FROM teams").fetchall()

    # Build set of all top-11 players across all teams
    top11_players = set()
    player_team_map = {}  # player_name -> team_name for bench players

    for team in teams:
        players = conn.execute("""
            SELECT r.player_name,
                   COALESCE(SUM(p.total_pts), 0) as total_pts
            FROM roster r
            LEFT JOIN player_match_points p ON r.player_name = p.player_name
            WHERE r.team_id = ? AND r.removed_date IS NULL
            GROUP BY r.player_name
            ORDER BY total_pts DESC
        """, (team["team_id"],)).fetchall()

        for i, p in enumerate(players):
            if i < 11:
                top11_players.add(p["player_name"])
            else:
                player_team_map[p["player_name"]] = team["team_name"]

    # Now find the bench player with the highest total_pts
    if not player_team_map:
        return None

    bench_names = list(player_team_map.keys())
    placeholders = ",".join("?" * len(bench_names))
    row = conn.execute(f"""
        SELECT player_name, COALESCE(SUM(total_pts), 0) as total_pts
        FROM player_match_points
        WHERE player_name IN ({placeholders})
        GROUP BY player_name
        ORDER BY total_pts DESC
        LIMIT 1
    """, bench_names).fetchone()

    if not row or row["total_pts"] == 0:
        # Fallback: return the first bench player even with 0 pts
        first_bench = bench_names[0]
        return {
            "player_name": first_bench,
            "total_pts": 0,
            "team_name": player_team_map[first_bench],
        }

    return {
        "player_name": row["player_name"],
        "total_pts": row["total_pts"],
        "team_name": player_team_map[row["player_name"]],
    }


def get_head_to_head(team1_name: str, team2_name: str) -> dict | None:
    """Return both teams' full player lists with stats, side by side."""
    conn = get_db()

    team1 = conn.execute(
        "SELECT team_id, team_name FROM teams WHERE team_name = ?", (team1_name,)
    ).fetchone()
    team2 = conn.execute(
        "SELECT team_id, team_name FROM teams WHERE team_name = ?", (team2_name,)
    ).fetchone()

    if not team1 or not team2:
        conn.close()
        return None

    def _get_team_players(team_id):
        return [dict(r) for r in conn.execute("""
            SELECT r.player_name,
                   COALESCE(SUM(p.total_pts), 0) as total_pts,
                   COALESCE(SUM(p.batting_pts), 0) as batting_pts,
                   COALESCE(SUM(p.bowling_pts), 0) as bowling_pts,
                   COALESCE(SUM(p.fielding_pts), 0) as fielding_pts,
                   COUNT(p.match_id) as matches_played
            FROM roster r
            LEFT JOIN player_match_points p ON r.player_name = p.player_name
            WHERE r.team_id = ? AND r.removed_date IS NULL
            GROUP BY r.player_name
            ORDER BY total_pts DESC
        """, (team_id,)).fetchall()]

    team1_players = _get_team_players(team1["team_id"])
    team2_players = _get_team_players(team2["team_id"])

    team1_top11 = team1_players[:11]
    team2_top11 = team2_players[:11]

    conn.close()

    return {
        "team1": {
            "team_name": team1["team_name"],
            "total_pts": sum(p["total_pts"] for p in team1_top11),
            "players": team1_players,
        },
        "team2": {
            "team_name": team2["team_name"],
            "total_pts": sum(p["total_pts"] for p in team2_top11),
            "players": team2_players,
        },
    }
