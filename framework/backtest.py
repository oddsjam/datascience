import logging
import os
import time
from collections import defaultdict
from datetime import datetime
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Manager, Pool
from typing import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
from functools import partial
import dask
import dask.dataframe as dd
import pandas as pd

from .utils import cache_odds, clean_old_games, dict_items_generator, gen_save_name, dict_to_oddts, normalize_id

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


def _process_file(
    games_ddf,
    start_date_map,
    output_folder,
    save_name,
    file_index,
    find_opportunities_function,
    interval=10,  # Run find_opportunities_function every 'interval' seconds
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
    last_processed_timestamp = None  # last timestamp processed for opportunities
    num_processed_timestamps = 0  # how many timestamps have been processed
    grouped_data = defaultdict(list)

    not_last_timestamp = False

    for i, record in enumerate(sorted_games_dict):
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


NORMALIZED_SPORTSBOOKS_TO_EXCLUDE = {
    "circa_vegas", # the closing line price is messed up
    "draftkings_pick_6_multipliers_", # this book isn't used on the tools right now
    "parlaye", # this book isn't used on the tools right now
    "underdog_fantasy_multipliers_", # this book isn't used on the tools right now
    "epick", # this book isn't used on the tools right now
    "suprabets", # this book isn't used on the tools right now
    "juicebet", # this book isn't used on the tools right now
    "dafabet", # this book isn't used on the tools right now
    "dazn_bet", # this book isn't used on the tools right now
    "blue_book", # this is a clone of FD
    "betr",
    "betr_picks",
    "betr_picks_all_",
}
NORMALIZED_SPORTSBOOK_SUBSTRING_TO_EXCLUDE = "_test_"


def _process_group(group_data, find_opportunities_function, normalized_market):
    """Process a single group and return opportunities"""
    # Convert group data to list of dictionaries (odds records)
    odds_list = group_data.sort_values(['sportsbook', 'outcome']).to_dict('records')
    odds = []
    
    for odd in odds_list:
        odd["points"] = odd.pop("olv_points", None)
        for k in ["olv_is_main", "clv_is_main", "grade", "desired", "outcome", 
                  "home_team", "away_team", "player_id", "clv_points", "olv_price", 
                  "olv_points", "opened_at", "fixture_id", "processed", "closed_at"]:
            odd.pop(k, None)
        odd["timestamp"] = odd.pop("start_date").to_pydatetime().timestamp()
        odd["price"] = odd.pop("clv_price", None)
        
        for k, v in odd.items():
            if pd.isna(v):
                odd[k] = None
        
        if odd["price"] is None:
            continue
            
        normalized_sportsbook = normalize_id(odd["sportsbook"])
        if normalized_sportsbook in NORMALIZED_SPORTSBOOKS_TO_EXCLUDE:
            continue
        if NORMALIZED_SPORTSBOOK_SUBSTRING_TO_EXCLUDE in normalized_sportsbook:
            continue
        odds.append(dict_to_oddts(odd))

    return find_opportunities_function({normalized_market: odds})


def _process_file_from_summary(
    summary_ddf,
    output_folder,
    find_opportunities_function,
    log_level,
    save_name,
    allowed_normalized_markets = set(),
    parallel = False,
):
    opportunities = []
    parquet_file_index = 0
    
    if allowed_normalized_markets:
        summary_ddf = summary_ddf[summary_ddf['normalized_market'].isin(allowed_normalized_markets)]

    summary_df = summary_ddf.compute()
    grouped = summary_df.groupby(['game_id', 'normalized_market'])
    
    # Prepare groups for parallel processing
    groups_to_process = []
    for (game_id, normalized_market), group_data in grouped:
        groups_to_process.append((group_data, normalized_market))
    
    file_name = f"{output_folder}/{save_name}/opportunities_{save_name}/partitions/file_0_batch_{parquet_file_index}.parquet"


    # Process groups in parallel
    if parallel:
        max_workers = min(mp.cpu_count(), len(groups_to_process))
    
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_group = {
                executor.submit(_process_group, group_data, find_opportunities_function, normalized_market): i
                for i, (group_data, normalized_market) in enumerate(groups_to_process)
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_group):
                opps = future.result()
                if opps:
                    opportunities.extend(opps)
                
                # Save when batch is full
                if len(opportunities) >= 2500000:
                    oppo_ddf = dd.from_pandas(pd.DataFrame(opportunities), chunksize=500000)
                    oppo_ddf.to_parquet(
                        file_name,
                        engine="pyarrow",
                    )
                    del oppo_ddf
                    opportunities = []
                    parquet_file_index += 1
        
        # Save remaining opportunities
        if opportunities:
            oppo_ddf = dd.from_pandas(pd.DataFrame(opportunities), chunksize=500000)
            oppo_ddf.to_parquet(
                file_name,
                engine="pyarrow",
            )
    else:
        for i, (group_data, normalized_market) in enumerate(groups_to_process):
            opps = _process_group(group_data, find_opportunities_function, normalized_market)
            if opps:
                opportunities.extend(opps)
            if len(opportunities) >= 2500000 or i == len(groups_to_process) - 1:
                oppo_ddf = dd.from_pandas(pd.DataFrame(opportunities), chunksize=500000)
                oppo_ddf.to_parquet(
                    file_name,
                    engine="pyarrow",
                )
                del oppo_ddf


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
    if log_queue:
        handler = QueueHandler(log_queue)
        logger.addHandler(handler)

    ddf = dd.read_parquet(file_path, engine="pyarrow")
    _process_file(
        ddf,
        start_date_map,
        output_folder,
        save_name,
        i,
        find_opportunities_function,
        interval=interval,
    )


def process_file_wrapper_from_summary(args):
    (
        summary_ddf,
        output_folder,
        find_opportunities_function,
        log_level,
        save_name,
        allowed_normalized_markets,
        parallel
    ) = args

    if not log_level:
        log_level = "INFO"

    logger = logging.getLogger()
    logger.setLevel(log_level)

    _process_file_from_summary(
        summary_ddf,
        output_folder,
        find_opportunities_function,
        log_level,
        save_name,
        allowed_normalized_markets,
        parallel,
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
    interval: int = 10,
):
    if log_level:
        root = logging.getLogger()
        root.setLevel(log_level)

    save_name = gen_save_name(sports, leagues, start_date, end_date)

    # Read summary
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
    file_paths = []
    for root_path, dirs, _ in os.walk(base_dir):
        for dir in dirs:
            if dir.endswith(".parquet"):
                file_paths.append(os.path.join(root_path, dir))

    start_time = time.perf_counter()
    with Manager() as manager:
        log_queue = manager.Queue()
        handler = logging.StreamHandler()
        listener = QueueListener(log_queue, handler)
        listener.start()

        args = [
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
            )
            for i, file_path in enumerate(file_paths)
        ]
        with Pool() as pool:
            pool.map(process_file_wrapper, args)

        listener.stop()

    logging.info(f"Processed all files in {time.perf_counter() - start_time} seconds")


def run_backtest_from_summary(
    sports: list[str],
    leagues: list[str],
    start_date: str,
    end_date: str,
    find_opportunities_function: Callable = lambda odds: [],
    data_folder: str = "../data",
    output_folder: str = "../output",
    log_level: str | None = None,
    allowed_normalized_markets: set[str] = set(),
    game_id: str | None = None,
    output_file_suffix: str = "",
    parallel: bool = False,
):
    if log_level:
        root = logging.getLogger()
        root.setLevel(log_level)

    save_name = gen_save_name(sports, leagues, start_date, end_date)

    # Read summary
    summary_path = f"{data_folder}/{save_name}/odds_summary_{save_name}.parquet"
    summary_ddf = dd.read_parquet(summary_path, engine="pyarrow")
    summary_ddf = summary_ddf.persist()
    if game_id:
        summary_ddf = summary_ddf[summary_ddf['game_id'] == game_id]
    # summary_ddf.info(memory_usage=True)
    # logging.info("Summary loaded")


    start_time = time.perf_counter()

    if output_file_suffix:
        save_name = f"{save_name}_{output_file_suffix}"
    process_file_wrapper_from_summary((
        summary_ddf,
        output_folder,
        find_opportunities_function,
        log_level,
        save_name,
        allowed_normalized_markets,
        parallel
    ))

    logging.info(f"Processed all files in {time.perf_counter() - start_time} seconds")