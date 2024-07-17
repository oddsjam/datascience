import logging
import re
from typing import Optional

import dask

from .models import OddTs

logger = logging.getLogger(__name__)


def gen_save_name(sports, leagues, start_date, end_date):
    if leagues:
        return f'{"_".join(sports)}_{"_".join(leagues)}_{start_date}_{end_date}'
    return f'{"_".join(sports)}_{start_date}_{end_date}'


def dict_items_generator(my_dict):
    for key, value in my_dict.items():
        yield key, value


def filter_and_convert(df, timestamp):
    return df[df["timestamp"] == timestamp].to_dict(orient="records")


def normalize_id(name: str | None, sep: str = "_") -> Optional[str]:
    """Normalize a id to be used as key"""
    if name is None:
        return None
    name = re.sub(r"[^a-zA-Z0-9\+\-]+", " ", name)
    return name.replace(" ", sep).lower()


filter_and_convert_delayed = dask.delayed(filter_and_convert)


def dict_to_oddts(record: dict) -> OddTs:
    odd = dict()
    for key, value in record.items():
        if key in ["locked"]:
            continue
        if key == "main":
            odd["is_main"] = value
            continue
        if key == "live":
            odd["is_live"] = value
            continue
        odd[key] = value

    return OddTs(**odd)


def cache_odds(game_id, norm_market, odds, active_odds_by_game_id):
    for odd in odds:
        norm_sportsbook = normalize_id(odd["sportsbook"])
        norm_name = normalize_id(odd["name"])
        locked = odd.get("locked", False)
        if game_id not in active_odds_by_game_id:
            active_odds_by_game_id[game_id] = {}
        if norm_market not in active_odds_by_game_id[game_id]:
            active_odds_by_game_id[game_id][norm_market] = {}
        if norm_sportsbook not in active_odds_by_game_id[game_id][norm_market]:
            active_odds_by_game_id[game_id][norm_market][norm_sportsbook] = {}
        if (
            norm_name
            not in active_odds_by_game_id[game_id][norm_market][norm_sportsbook]
        ):
            active_odds_by_game_id[game_id][norm_market][norm_sportsbook][
                norm_name
            ] = {}
        if locked:
            del active_odds_by_game_id[game_id][norm_market][norm_sportsbook][norm_name]
        else:
            active_odds_by_game_id[game_id][norm_market][norm_sportsbook][
                norm_name
            ] = dict_to_oddts(odd)


def clean_old_games(game_id_by_start_time, active_odds_by_game_id, timestamp):
    old_keys = list(
        set([key for key in game_id_by_start_time.keys() if key < timestamp])
    )
    for old_key in old_keys:
        if old_key == 1712013000.0:
            logging.info(f"Found old key {old_key} at timestamp {timestamp}")

        for game_id in game_id_by_start_time[old_key]:
            if game_id in active_odds_by_game_id:
                logging.info(
                    f"Deleting valid odds for {game_id} at timestamp {timestamp}"
                )
                del active_odds_by_game_id[game_id]
        if old_key in game_id_by_start_time:
            logging.info(f"Deleting {old_key} at timestamp {timestamp}")
            del game_id_by_start_time[old_key]
