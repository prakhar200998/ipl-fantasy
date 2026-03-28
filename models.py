"""Dataclasses for normalized match data."""
from dataclasses import dataclass, field


@dataclass
class BattingEntry:
    player: str
    runs: int = 0
    balls: int = 0
    fours: int = 0
    sixes: int = 0
    dismissed: bool = False


@dataclass
class BowlingEntry:
    player: str
    balls: int = 0
    runs: int = 0
    dots: int = 0
    wickets: int = 0
    lbw_bowled: int = 0
    # key: "{team}_{over_num}", value: {"balls": int, "runs": int}
    overs_detail: dict = field(default_factory=dict)


@dataclass
class FieldingEntry:
    player: str
    catches: int = 0
    runouts: int = 0
    stumpings: int = 0


@dataclass
class MatchScorecard:
    match_id: str
    date: str
    teams: list[str]
    venue: str
    status: str  # "complete", "in_progress", "upcoming"
    playing_xi: set[str] = field(default_factory=set)
    batting: dict[str, BattingEntry] = field(default_factory=dict)
    bowling: dict[str, BowlingEntry] = field(default_factory=dict)
    fielding: dict[str, FieldingEntry] = field(default_factory=dict)
    batters_who_batted: set[str] = field(default_factory=set)
