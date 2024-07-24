import logging
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import Callable

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd

from .utils import cache_odds, clean_old_games, dict_items_generator, gen_save_name

dask.config.set({"optimization.fuse.active": True})


def is_difference_less_than_x_seconds(timestamp1, timestamp2, x=10):
    if isinstance(timestamp1, str):
        timestamp1 = datetime.fromisoformat(timestamp1)
    elif isinstance(timestamp1, int):
        timestamp1 = datetime.fromtimestamp(timestamp1)
    if isinstance(timestamp2, str):
        timestamp2 = datetime.fromisoformat(timestamp2)
    elif isinstance(timestamp2, int):
        timestamp2 = datetime.fromtimestamp(timestamp2)

    difference = abs(timestamp1 - timestamp2)

    return difference.total_seconds() < 10


def _process_file(
    games_ddf,
    start_date_map,
    output_folder,
    save_name,
    file_index,
    find_opportunities_function,
):
    opportunities = []
    active_odds_by_game_id: dict[int, dict[str, dict[str, dict[str, float]]]] = {}
    game_id_by_start_time: dict[float, set[int]] = {}
    timestamps = sorted(games_ddf["timestamp"].unique().compute())
    logging.debug(f"Found {len(timestamps)} timestamps")

    games_dict = games_ddf.compute().to_dict("records")
    sorted_games_dict = sorted(games_dict, key=lambda x: x["timestamp"])

    parquet_file_index = 0
    last_timestamp = None  # last timestamp
    num_processed_timestamps = 0  # how many timestamps have been processed
    grouped_data = defaultdict(list)

    for i, record in enumerate(sorted_games_dict):
        current_timestamp = record["timestamp"]

        if i == 0:
            last_timestamp = current_timestamp

        if current_timestamp == last_timestamp:
            game_id = record["game_id"]
            normalized_market = record["normalized_market"]
            grouped_data[(game_id, normalized_market)].append(record)

            # if it's the last record, process it
            if i < len(sorted_games_dict) - 1:
                continue

        if current_timestamp < last_timestamp:
            raise ValueError("Timestamps are not sorted")

        start_time_timestamp = time.perf_counter()
        for j, (key, odds) in enumerate(dict_items_generator(grouped_data)):
            game_id, normalized_market = key
            start_date = start_date_map[game_id]
            start_date_ts = datetime.fromisoformat(start_date).timestamp()

            if start_date_ts not in game_id_by_start_time:
                game_id_by_start_time[start_date_ts] = set()

            game_id_by_start_time[start_date_ts].add(game_id)

            cache_odds(game_id, normalized_market, odds, active_odds_by_game_id)
            odds_by_market = active_odds_by_game_id[game_id][normalized_market]

            odds_to_check = {}
            for sportsbook, names in odds_by_market.items():
                odds_to_check[sportsbook] = [odd for odd in names.values()]

            logging.debug(
                f"Processing game {game_id} market {normalized_market} at {last_timestamp}"
            )
            tmp_opportunities = find_opportunities_function(odds_to_check)
            opportunities.extend(tmp_opportunities)
        logging.debug(
            f"Processed all odd in {time.perf_counter() - start_time_timestamp} seconds"
        )

        # clean old games
        clean_timestamp = time.perf_counter()
        made_changes = clean_old_games(
            game_id_by_start_time, active_odds_by_game_id, last_timestamp
        )
        if made_changes:
            logging.debug(
                f"Cleaned old games in {time.perf_counter() - clean_timestamp} seconds"
            )

        # reset after processing
        last_timestamp = current_timestamp
        num_processed_timestamps += 1
        grouped_data = defaultdict(list)
        game_id = record["game_id"]
        normalized_market = record["normalized_market"]
        grouped_data[(game_id, normalized_market)].append(record)

        if num_processed_timestamps % 1000 == 0:
            logging.info(f"Analysed {i+1}/{len(sorted_games_dict)} odds")
        if i == len(sorted_games_dict) - 1:
            logging.info(f"Analysed {i+1}/{len(sorted_games_dict)} odds")
        if len(opportunities) >= 2500000 or i == len(sorted_games_dict) - 1:
            logging.info("Writing to parquet file")
            oppo_ddf = dd.from_pandas(pd.DataFrame(opportunities), chunksize=500000)
            oppo_ddf.to_parquet(
                f"{output_folder}/{save_name}/opportunities_{save_name}/partitions/file_{file_index}_batch_{parquet_file_index}.parquet",
                engine="pyarrow",
            )
            del oppo_ddf
            opportunities = []
            parquet_file_index += 1

        logging.debug(
            f"Processed timestamp {i}/{len(sorted_games_dict)} in {time.perf_counter() - start_time_timestamp} seconds"
        )


def run_backtest(
    sports: list[str],
    leagues: list[str],
    start_date: str,
    end_date: str,
    find_opportunities_function: Callable = lambda odds: [],
    data_folder: str = "../data",
    output_folder: str = "../output",
    log_level: str | None = None,
):
    if log_level:
        root = logging.getLogger()
        root.setLevel(log_level)

    save_name = gen_save_name(sports, leagues, start_date, end_date)

    # read summary
    summary_path = f"{data_folder}/{save_name}/odds_summary_{save_name}.parquet"
    summary_ddf = dd.read_parquet(summary_path, engine="pyarrow")
    summary_ddf = summary_ddf.persist()
    summary_ddf.info(memory_usage=True)
    logging.info("Summary loaded")
    start_date_map = {
        s["game_id"]: s["start_date"] for s in summary_ddf.compute().to_dict("records")
    }
    logging.info("Start date map created")

    base_dir = f"{data_folder}/{save_name}/odds_ts_{save_name}/partitions"
    i = 0
    start_time = time.perf_counter()
    for root_path, dirs, files in os.walk(base_dir):
        total_files = len(dirs)
        for dir in dirs:
            if dir.endswith(".parquet"):
                start_time_internal = time.perf_counter()
                file_path = os.path.join(root_path, dir)
                logging.info(f"reading file: {file_path}")
                ddf = dd.read_parquet(file_path, engine="pyarrow")
                ddf = ddf.replace([np.nan], [None])
                ddf = ddf.persist()
                _process_file(
                    ddf,
                    start_date_map,
                    file_index=i,
                    save_name=save_name,
                    output_folder=output_folder,
                    find_opportunities_function=find_opportunities_function,
                )
                i += 1
                logging.info(
                    f"Processed batch {i}/{total_files} in {time.perf_counter() - start_time_internal} seconds"
                )
    logging.info(f"Processed all files in {time.perf_counter() - start_time} seconds")
