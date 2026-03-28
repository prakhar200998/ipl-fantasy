"""Abstract base class for data source adapters."""
from abc import ABC, abstractmethod
from models import MatchScorecard


class DataSourceAdapter(ABC):
    @abstractmethod
    def get_scorecard(self, match_id: str) -> MatchScorecard | None:
        """Get scorecard for a specific match."""
        ...

    @abstractmethod
    def get_match_list(self, season: str) -> list[dict]:
        """Get list of matches for a season. Returns list of {match_id, date, teams, status}."""
        ...
