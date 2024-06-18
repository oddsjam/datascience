import dataclasses
import json
from datetime import datetime, timezone


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
    price: int | None = None
    is_main: bool | None = None
    is_live: bool | None = None
    timestamp: float | None = None
    player_id: str | None = None
    tournament: str | None = None
    consensus_line: str | None = None
    consensus_line_name: str | None = None
    selection: str | None = None
    normalized_selection: str | None = None
    selection_line: str | None = None
    selection_points: float | None = None
    deep_link_rule: str | None = None
    promo_line: bool | None = None

    deep_link_info: dict | None = None

    # Deprecated Fields
    speed: int | None = None
    checked_date: datetime | None = None
    ts_key: str | None = None

    def __post_init__(self):
        if self.points is not None:
            self.points = float(self.points)
        if self.price is not None:
            self.price = float(self.price)
        if self.is_main is not None and not isinstance(self.is_main, bool):
            self.is_main = self.is_main == "1"
        if self.is_live is not None and not isinstance(self.is_live, bool):
            self.is_live = self.is_live == "1"
        if self.timestamp is not None:
            self.timestamp = float(self.timestamp)
        if self.selection_points is not None:
            self.selection_points = float(self.selection_points)

    def json(self):
        return json.dumps(dataclasses.asdict(self))

    def all_player_ids(self):
        return self.player_id.split("+")
