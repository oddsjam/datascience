import dataclasses
import json

from .utils import normalize_id


@dataclasses.dataclass(slots=True)
class OddTs:
    """Model that represents an Odd."""

    game_id: str
    sportsbook: str
    name: str
    market: str
    id: str | None = None
    sport: str | None = None
    league: str | None = None
    points: float | None = None
    price: float | None = None
    is_main: bool | None = None
    is_live: bool | None = None
    timestamp: float | None = None
    tournament: str | None = None
    selection: str | None = None
    normalized_selection: str | None = None
    selection_line: str | None = None
    selection_points: float | None = None
    normalized_sport: str | None = None
    normalized_league: str | None = None
    normalized_market: str | None = None
    normalized_name: str | None = None
    normalized_sportsbook: str | None = None

    def __post_init__(self):
        if self.points is not None and not isinstance(self.points, float):
            self.points = float(self.points)
        if self.price is not None and not isinstance(self.price, float):
            self.price = float(self.price)
        if self.timestamp is not None and not isinstance(self.timestamp, float):
            self.timestamp = float(self.timestamp)
        if self.is_main is not None and not isinstance(self.is_main, bool):
            self.is_main = self.is_main == "1"
        if self.is_live is not None and not isinstance(self.is_live, bool):
            self.is_live = self.is_live == "1"
        if self.selection_points is not None and not isinstance(
            self.selection_points, float
        ):
            self.selection_points = float(self.selection_points)
        if self.normalized_sport is None and self.sport is not None:
            self.normalized_sport = normalize_id(self.sport)
        if self.normalized_league is None and self.league is not None:
            self.normalized_league = normalize_id(self.league)
        if self.normalized_market is None and self.market is not None:
            self.normalized_market = normalize_id(self.market)
        if self.normalized_name is None and self.name is not None:
            self.normalized_name = normalize_id(self.name)

    def json(self):
        return json.dumps(dataclasses.asdict(self))
