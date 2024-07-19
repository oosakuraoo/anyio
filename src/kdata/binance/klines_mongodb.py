import time
from datetime import datetime, timedelta, timezone
from binance.client import Client
import pandas as pd
import numpy as np
import math
import conf.binance.klines as coin_config
from src.lib.mongo.db import MongoDBManager
import logging

# TODO


logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

mongo = MongoDBManager()


class BinanceKlinesToDB:
    def __init__(self, symbol, time_frame):
        self.symbol = symbol
        self.time_frame = time_frame

        self.api_key = coin_config.api_key
        self.api_secret = coin_config.api_secret
        if not self.api_key or not self.api_secret:
            raise ValueError(
                "BINANCE_API_KEY and BINANCE_API_SECRET environment variables must be set."
            )

        self.client = Client(self.api_key, self.api_secret)

    def __get_latest_klines(self):
        latest_timestamp = self.__get_last_saved_timestamp()
        start_str = self.__format_start_time(latest_timestamp)
        klines = self.client.get_historical_klines(
            self.symbol, self.time_frame, start_str
        )
        return klines

    def __get_last_file(self):
        try:
            with mongo.connect() as query_obj:
                max_id = mongo.find_max(
                    f"{self.symbol}_{self.time_frame}",
                    {},
                    sort=[("_id", -1)],
                )
                if max_id is None:
                    return None
                return f"{self.symbol}_{self.time_frame}_{max_id['_id']}"
        except FileNotFoundError:
            return None

    def __get_last_saved_timestamp(self):
        try:
            latest_file = self.__get_last_file()
            if latest_file is None:
                return None

            with mongo.connect() as query_obj:
                last_timestamp = mongo.find_max(
                    latest_file,
                    {},
                    sort=[("open_time", -1)],
                )
            with mongo.connect() as delete_obj:
                mongo.delete_one(latest_file, {"open_time": last_timestamp})

            return last_timestamp
        except FileNotFoundError:
            return None

    def __format_start_time(self, timestamp):
        if timestamp is None:
            return "2017-10-10 00:00:00"  # None  # "2017-10-10 00:00:00"
        else:
            dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def __save_klines_to_csv(self, klines):
        df = pd.DataFrame(
            klines,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_asset_volume",
                "num_trades",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "ignore",
            ],
        )
        # df.set_index("open_time")

        #
        chunk_size = 20000
        df_len = len(df)
        old_df_len = 0
        subset_df_len = 0
        start_i = 1
        #
        latest_file = self.__get_last_file()
        if latest_file is not None:
            with mongo.connect() as query_count:
                old_df_len = mongo.counts(latest_file)

        if old_df_len != 0 and old_df_len < chunk_size:
            subset_df_len = chunk_size - old_df_len
            if subset_df_len > df_len:
                subset_df_len = df_len
            subset_df = df.iloc[:subset_df_len]

            start_i = int(latest_file.split("_")[-1]) + 1

            logging.debug(
                f" latest_file: {latest_file}, df_len: {df_len} , old_df_len: {old_df_len}, subset_df_len: {subset_df_len}, start_i: {start_i}"
            )

            mongo.insert_many(latest_file, subset_df.to_dict("records"))

        if subset_df_len >= df_len:
            return

        merged_data = df
        if subset_df_len > 0:
            merged_data = df.iloc[:-subset_df_len]
        merged_data_len = len(merged_data)
        if merged_data_len == 0:
            return

        if chunk_size >= merged_data_len:
            with mongo.connect() as insert_obj:
                file_name = f"{self.symbol}_{self.time_frame}"
                mongo.insert_one(file_name, {"id": start_i})

            with mongo.connect() as db_obj:
                file_name = f"{self.symbol}_{self.time_frame}_{start_i}"
                mongo.insert_many(file_name, merged_data.to_dict("records"))

            return

        chunks = [
            chunk
            for chunk in np.array_split(
                merged_data, math.ceil(merged_data_len / chunk_size)
            )
        ]
        for i, chunk in enumerate(chunks, start=start_i):
            with mongo.connect() as insert_obj:
                file_name = f"{self.symbol}_{self.time_frame}"
                mongo.insert_one(file_name, {"id": i})

            with mongo.connect() as db_obj:
                file_name = f"{self.symbol}_{self.time_frame}_{i}"
                mongo.insert_many(file_name, merged_data.to_dict("records"))

    def update_and_save_klines(self):
        klines = self.__get_latest_klines()
        if klines:
            self.__save_klines_to_csv(klines)

    # def run_periodic_updates(self, interval_seconds=60):
    #     def job():
    #         self.update_and_save_klines()

    #     schedule.every(interval_seconds).seconds.do(job)

    #     while True:
    #         schedule.run_pending()
    #         time.sleep(3)
