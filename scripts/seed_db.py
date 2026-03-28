#!/usr/bin/env python3
"""Seed the database with teams, rosters, and IPL 2026 match data from CricketData.org API."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import init_db, seed_teams, wipe_all, upsert_match, bulk_upsert_player_points
from teams import TEAMS
from adapters.cricketdata import CricketDataAdapter
from scoring import calculate_fantasy_points
from config import SEASON


def main():
    print("Initializing database...")
    init_db()

    print("Wiping old data...")
    wipe_all()

    # Seed teams — use display names directly (CricketData.org uses full names)
    print(f"Seeding {len(TEAMS)} teams...")
    seed_teams(TEAMS)

    # Fetch IPL 2026 matches from CricketData.org API
    adapter = CricketDataAdapter()
    matches = adapter.get_match_list(SEASON)
    completed = [m for m in matches if m["status"] == "complete"]
    in_progress = [m for m in matches if m["status"] == "in_progress"]

    print(f"Found {len(matches)} total IPL 2026 matches")
    print(f"  {len(completed)} completed, {len(in_progress)} in progress, {len(matches) - len(completed) - len(in_progress)} upcoming")

    # Process completed matches
    processed = 0
    for match in completed:
        scorecard = adapter.get_scorecard(match["match_id"])
        if not scorecard:
            print(f"  SKIP (no scorecard): {match.get('name', match['match_id'])}")
            continue
        points = calculate_fantasy_points(scorecard)
        upsert_match(match["match_id"], match["date"], match["teams"], match["venue"], "complete")
        bulk_upsert_player_points(match["match_id"], points)
        processed += 1
        print(f"  OK: {match.get('name', match['match_id'])}")

    # Process in-progress matches
    for match in in_progress:
        scorecard = adapter.get_scorecard(match["match_id"])
        if not scorecard:
            print(f"  SKIP (no scorecard): {match.get('name', match['match_id'])}")
            continue
        points = calculate_fantasy_points(scorecard)
        upsert_match(match["match_id"], match["date"], match["teams"], match["venue"], "in_progress")
        bulk_upsert_player_points(match["match_id"], points)
        processed += 1
        print(f"  OK (live): {match.get('name', match['match_id'])}")

    print(f"\nDone! Processed {processed} matches with scorecards.")


if __name__ == "__main__":
    main()
