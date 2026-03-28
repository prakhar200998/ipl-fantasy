#!/usr/bin/env python3
"""Seed the database with teams, rosters, and historical match data."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import init_db, seed_teams, upsert_match, bulk_upsert_player_points
from teams import TEAMS
from name_mapping import get_cricsheet_name
from adapters.cricsheet import CricsheetAdapter
from scoring import calculate_fantasy_points
from config import CRICSHEET_DATA_DIR, SEASON


def main():
    print("Initializing database...")
    init_db()

    # Seed teams with cricsheet names in roster
    print(f"Seeding {len(TEAMS)} teams...")
    teams_with_cs_names = {}
    for team_name, players in TEAMS.items():
        teams_with_cs_names[team_name] = [get_cricsheet_name(p) for p in players]
    seed_teams(teams_with_cs_names)

    # Process historical matches
    adapter = CricsheetAdapter(CRICSHEET_DATA_DIR)
    matches = adapter.get_match_list(SEASON)
    print(f"Found {len(matches)} matches for season {SEASON}")

    for i, match in enumerate(matches):
        scorecard = adapter.get_scorecard(match["match_id"])
        if not scorecard:
            continue
        points = calculate_fantasy_points(scorecard)
        upsert_match(match["match_id"], match["date"], match["teams"], match["venue"], "complete")
        bulk_upsert_player_points(match["match_id"], points)
        if (i + 1) % 10 == 0:
            print(f"  Processed {i + 1}/{len(matches)} matches...")

    print(f"Done! Seeded {len(matches)} matches.")


if __name__ == "__main__":
    main()
