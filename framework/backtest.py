import glob
import logging
import os
import time
from collections import defaultdict
from datetime import datetime
from logging.handlers import QueueListener
from multiprocessing import Manager, Pool
from typing import Callable

import dask
import numpy as np
import pandas as pd

from .utils import (
    cache_odds,
    clean_old_games,
    dict_items_generator,
    gen_save_name,
    normalize_id,
)

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

    return difference.total_seconds() < x


def load_odds_df(data_folder, save_name, bet_type="moneyline"):
    """Loading odds for the particular game.

    Args:
        data_folder: path to data folder
        save_name: name of the folder in the data folder
        bet_type: type of bet to store

    Return:
        pd.Dataframe containing all the odds for the particular game.
    """
    odds_ddf = pd.DataFrame()
    odds_file_path = f"{data_folder}/{save_name}/odds_ts_{save_name}/partitions/*"
    odds_files = glob.glob(odds_file_path)
    for file in odds_files:
        ddf = pd.read_parquet(file)
        ddf = ddf.replace([np.nan], [None])
        odds_ddf = pd.concat([odds_ddf, ddf])
    odds_df = odds_ddf
    odds_df = odds_df[odds_df["normalized_market"] == bet_type]
    odds_df["normalized_sportsbook"] = odds_df["sportsbook"].apply(
        lambda x: normalize_id(x)
    )
    return odds_df


def load_odds_df_range(
    data_folder, sports, leagues, start_dates, end_dates, bet_type="moneyline"
):
    """Loading odds for all the games in start and end dates.

    Args:
        data_folder: path to data folder
        sports: name of sport
        leagues: name of league
        start_dates: list of start dates
        end_dates: list of end dates
        bet_type: type of bet to store

    Return:
        pd.Dataframe containing all the odds for all games concatenated.
    """
    odds_df = pd.DataFrame()
    for start_date, end_date in zip(start_dates, end_dates):
        save_name = gen_save_name(sports, leagues, start_date, end_date)
        df = load_odds_df(data_folder, save_name, bet_type)
        odds_df = pd.concat([odds_df, df], axis=0)

    return odds_df


def load_summary_df_range(data_folder, sports, leagues, start_dates, end_dates):
    """Loading odds for all the games in start and end dates.

    Args:
        data_folder: path to data folder
        sports: name of sport
        leagues: name of league
        start_dates: list of start dates
        end_dates: list of end dates

    Return:
        pd.Dataframe containing all the summary data for all games concatenated.
    """
    summary_df = pd.DataFrame()
    for start_date, end_date in zip(start_dates, end_dates):
        save_name = gen_save_name(sports, leagues, start_date, end_date)
        summary_path = f"{data_folder}/{save_name}/odds_summary_{save_name}.parquet"
        df = pd.read_parquet(summary_path)
        summary_df = pd.concat([summary_df, df])
    return summary_df


def divide_odds_game_id(odds_df, bet_type):
    """Divide odds df into list of dataframes containing odds for individual games.

    Args:
    odds_df: Dataframe containing odds for all the games.

    Return:
    List of dataframe divided by game_id.
    """
    logging.info(f"num_games {len(odds_df['game_id'].unique())}")
    grouped = odds_df.groupby(by="game_id")
    games_files = []
    for game_id, group in grouped:
        group.to_parquet(f"/tmp/{bet_type}_{game_id}.parquet")
        games_files += [f"/tmp/{bet_type}_{game_id}.parquet"]
    return games_files


def _process_file(
    games_ddf,
    start_date_map,
    output_folder,
    save_name,
    file_index,
    find_opportunities_function,
    interval=10,  # Run find_opportunities_function every 'interval' seconds
):
    logging.info("Starting.......")
    opportunities = []
    active_odds_by_game_id: dict[int, dict[str, dict[str, dict[str, float]]]] = {}
    game_id_by_start_time: dict[float, set[int]] = {}
    timestamps = sorted(games_ddf["timestamp"].unique())
    logging.debug(f"Found {len(timestamps)} timestamps")
    games_df_computed = games_ddf
    # Uncomment below line when you want to run on a subset
    # games_df_computed = games_df_computed.iloc[:20000, :]
    games_df_computed = games_df_computed[games_df_computed["name"].notna()]
    games_df_computed = games_df_computed[games_df_computed["market"].notna()]
    games_dict = games_df_computed.to_dict("records")
    sorted_games_dict = sorted(games_dict, key=lambda x: x["timestamp"])

    parquet_file_index = 0
    last_timestamp = None  # last timestamp
    last_processed_timestamp = None  # last timestamp processed for opportunities
    num_processed_timestamps = 0  # how many timestamps have been processed
    grouped_data = defaultdict(list)

    not_last_timestamp = False

    for i, record in enumerate(sorted_games_dict):
        if i % 1000 == 0:
            logging.info(f"{i} {len(sorted_games_dict)}")
        not_last_timestamp = i < len(sorted_games_dict) - 1
        current_timestamp = record["timestamp"]

        if i == 0:
            last_timestamp = current_timestamp

        if current_timestamp == last_timestamp:
            game_id = record["game_id"]
            normalized_market = record["normalized_market"]
            grouped_data[(game_id, normalized_market)].append(record)

            # if it's the last record, process it otherwise skip it
            if not_last_timestamp:
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

            if (
                last_processed_timestamp is not None
                and is_difference_less_than_x_seconds(
                    last_processed_timestamp,
                    last_timestamp,
                    interval,
                )
                and not_last_timestamp
            ):
                logging.debug("skipping find_opportunities_function")
                continue
            logging.debug("running find_opportunities_function")
            tmp_opportunities = find_opportunities_function(odds_to_check)
            opportunities.extend(tmp_opportunities)
            last_processed_timestamp = last_timestamp
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
        logging.debug("Reset after processing")
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
        if len(opportunities) > 0 and (
            len(opportunities) >= 100 or i == len(sorted_games_dict) - 1
        ):
            logging.info("Writing to parquet file")
            oppo_ddf = pd.DataFrame(opportunities)
            os.makedirs(
                f"{output_folder}/{save_name}/opportunities_{save_name}/partitions/",
                exist_ok=True,
            )
            oppo_ddf.to_parquet(
                f"{output_folder}/{save_name}/opportunities_{save_name}/partitions/file_{file_index}_batch_{parquet_file_index}.parquet"
            )
            # oppo_ddf.to_csv(f'out_oppo_{parquet_file_index}.csv')
            del oppo_ddf
            opportunities = []
            parquet_file_index += 1

        logging.debug(
            f"Processed timestamp {i}/{len(sorted_games_dict)} in {time.perf_counter() - start_time_timestamp} seconds"
        )
    logging.info(f"file_{file_index} completed.")


def process_file_wrapper(args):
    (
        file_path,
        i,
        start_date_map,
        output_folder,
        save_name,
        find_opportunities_function,
        log_queue,
        log_level,
        interval,
    ) = args

    if not log_level:
        log_level = "INFO"

    # Configure logging for the worker process
    logger = logging.getLogger()
    logger.setLevel(log_level)
    # handler = QueueHandler(log_queue)
    # logger.addHandler(handler)

    ddf = pd.read_parquet(file_path)

    _process_file(
        ddf,
        start_date_map,
        output_folder,
        save_name,
        i,
        find_opportunities_function,
        interval=interval,
    )


def run_backtest(
    sports: list[str],
    leagues: list[str],
    bet_type: str,
    start_dates: str,
    end_dates: str,
    find_opportunities_function: Callable = lambda odds: [],
    data_folder: str = "../data",
    output_folder: str = "../output",
    log_level: str | None = None,
    interval: int = 10,
    run_multiprocessing: bool = False,
    num_processes: int = 5,
):
    if log_level:
        root = logging.getLogger()
        root.setLevel(log_level)

    summary_ddf = load_summary_df_range(
        data_folder, sports, leagues, start_dates, end_dates
    )
    # summary_ddf = summary_ddf.persist()
    summary_ddf.info(memory_usage=True)
    logging.info("Summary loaded")

    # Start date map with game against its start date
    # We only load pre game bets
    start_date_map = {
        s["game_id"]: s["start_date"] for s in summary_ddf.to_dict("records")
    }
    logging.info("Start date map created")

    save_name = gen_save_name(
        sports, leagues, start_date=min(start_dates), end_date=max(end_dates)
    )
    start_time = time.perf_counter()

    odds_df = load_odds_df_range(
        data_folder, sports, leagues, start_dates, end_dates, bet_type
    )
    games_files = divide_odds_game_id(odds_df, bet_type)

    with Manager() as manager:
        log_queue = manager.Queue()
        handler = logging.StreamHandler()
        listener = QueueListener(log_queue, handler)
        listener.start()

        args = [
            (
                game_file,
                i,
                start_date_map,
                output_folder,
                save_name,
                find_opportunities_function,
                log_queue,
                log_level,
                interval,
            )
            for i, game_file in enumerate(games_files)
        ]
        logging.info(f"num_args {len(args)}")
        if run_multiprocessing:
            logging.info("Running with multiprocessing")
            with Pool(num_processes) as pool:
                pool.map(process_file_wrapper, args)
        else:
            logging.info("Running with single process")
            for argument in args:
                process_file_wrapper(argument)

        listener.stop()

    logging.info(f"Processed all files in {time.perf_counter() - start_time} seconds")
