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
            added_date TEXT DEFAULT '2025-01-01',
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
    """Seed teams and rosters from the TEAMS dict."""
    conn = get_db()
    for team_name, players in teams_dict.items():
        conn.execute(
            "INSERT OR IGNORE INTO teams (team_name) VALUES (?)",
            (team_name,)
        )
        team_id = conn.execute(
            "SELECT team_id FROM teams WHERE team_name = ?", (team_name,)
        ).fetchone()["team_id"]
        for player in players:
            existing = conn.execute(
                "SELECT 1 FROM roster WHERE team_id = ? AND player_name = ? AND removed_date IS NULL",
                (team_id, player)
            ).fetchone()
            if not existing:
                conn.execute(
                    "INSERT INTO roster (team_id, player_name) VALUES (?, ?)",
                    (team_id, player)
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


def upsert_player_points(match_id: str, player_name: str, pts: dict):
    conn = get_db()
    conn.execute("""
        INSERT INTO player_match_points (match_id, player_name, batting_pts, bowling_pts, fielding_pts, total_pts, breakdown_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(match_id, player_name) DO UPDATE SET
            batting_pts = excluded.batting_pts,
            bowling_pts = excluded.bowling_pts,
            fielding_pts = excluded.fielding_pts,
            total_pts = excluded.total_pts,
            breakdown_json = excluded.breakdown_json
    """, (match_id, player_name, pts["batting_pts"], pts["bowling_pts"],
          pts["fielding_pts"], pts["total_pts"], json.dumps(pts["breakdown"])))
    conn.commit()
    conn.close()


def bulk_upsert_player_points(match_id: str, all_points: dict[str, dict]):
    """Upsert all player points for a match in a single transaction."""
    conn = get_db()
    for player_name, pts in all_points.items():
        conn.execute("""
            INSERT INTO player_match_points (match_id, player_name, batting_pts, bowling_pts, fielding_pts, total_pts, breakdown_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(match_id, player_name) DO UPDATE SET
                batting_pts = excluded.batting_pts,
                bowling_pts = excluded.bowling_pts,
                fielding_pts = excluded.fielding_pts,
                total_pts = excluded.total_pts,
                breakdown_json = excluded.breakdown_json
        """, (match_id, player_name, pts["batting_pts"], pts["bowling_pts"],
              pts["fielding_pts"], pts["total_pts"], json.dumps(pts["breakdown"])))
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

    players = conn.execute("""
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
    """, (team["team_id"],)).fetchall()

    player_list = [dict(p) for p in players]
    top11 = player_list[:11]
    top11_total = sum(p["total_pts"] for p in top11)

    conn.close()
    return {
        "team_name": team["team_name"],
        "total_pts": top11_total,
        "players": player_list,
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
