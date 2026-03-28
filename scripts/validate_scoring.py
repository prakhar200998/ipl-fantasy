#!/usr/bin/env python3
"""Validate scoring engine against ipl2025_fantasy_points.csv ground truth."""
import sys
import os
import csv
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from adapters.cricsheet import CricsheetAdapter
from scoring import calculate_fantasy_points
from config import CRICSHEET_DATA_DIR


def main():
    # Load ground truth
    csv_path = os.path.join(CRICSHEET_DATA_DIR, "ipl2025_fantasy_points.csv")
    ground_truth = {}
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            ground_truth[row["Player"]] = int(row["Total Fantasy Points"])

    # Calculate from engine
    adapter = CricsheetAdapter(CRICSHEET_DATA_DIR)
    matches = adapter.get_match_list("2025")
    print(f"Processing {len(matches)} matches...")

    total_points = defaultdict(int)
    for match in matches:
        scorecard = adapter.get_scorecard(match["match_id"])
        if not scorecard:
            continue
        points = calculate_fantasy_points(scorecard)
        for player, pts in points.items():
            total_points[player] += pts["total_pts"]

    # Compare
    mismatches = 0
    matched = 0
    for player, expected in sorted(ground_truth.items(), key=lambda x: -x[1]):
        actual = total_points.get(player, 0)
        if actual != expected:
            diff = actual - expected
            print(f"  MISMATCH: {player:<25s} expected={expected:5d}  got={actual:5d}  diff={diff:+d}")
            mismatches += 1
        else:
            matched += 1

    # Check for players in engine but not in ground truth
    extra = set(total_points.keys()) - set(ground_truth.keys())
    if extra:
        print(f"\n  {len(extra)} players in engine but not in CSV (OK — they may have 0 pts or different season)")

    print(f"\nResults: {matched} matched, {mismatches} mismatched out of {len(ground_truth)} players")
    if mismatches == 0:
        print("ALL GOOD — scoring engine matches ground truth perfectly!")
    return mismatches == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
