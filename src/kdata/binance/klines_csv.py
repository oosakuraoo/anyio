import os
import schedule
import time
from datetime import datetime, timedelta, timezone
from binance.client import Client
import pandas as pd
import numpy as np
import math
import conf.binance.klines as coin_config
import conf.config as config
import glob
from natsort import natsorted
from src.lib.log import Logger, LogLevel

log = Logger(level=LogLevel.DEBUG.value)


class BinanceKlinesToCSV:
    def __init__(self, symbol, time_frame):
        self.symbol = symbol
        self.time_frame = time_frame
        self.output_dir = config.coin_output_dir + symbol
        self.file_prefix = f"{self.symbol}_{self.time_frame}_"

        self.api_key = coin_config.api_key
        self.api_secret = coin_config.api_secret
        if not self.api_key or not self.api_secret:
            raise ValueError(
                "BINANCE_API_KEY and BINANCE_API_SECRET environment variables must be set."
            )

        self.client = Client(self.api_key, self.api_secret)

        self.ctime = time.time()

    def __get_latest_klines(self):
        latest_timestamp = self.__get_last_saved_timestamp()
        start_str = self.__format_start_time(latest_timestamp)
        klines = self.client.get_historical_klines(
            self.symbol, self.time_frame, start_str
        )
        return klines

    def __get_last_file(self):
        try:
            file_pattern = f"{self.file_prefix}*.csv"
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
            arr_files = glob.glob(os.path.join(self.output_dir, file_pattern))
            sorted_csv_files = natsorted(arr_files)
            files_len = len(sorted_csv_files)
            if files_len == 0:
                return None
            latest_file = max(
                sorted_csv_files,
                key=os.path.getctime,
            )
            if files_len > 1:
                df = pd.read_csv(latest_file)
                if len(df) < 2:
                    os.remove(latest_file)
                    return self.__get_last_file()
            return latest_file
        except FileNotFoundError:
            return None

    def __get_last_saved_timestamp(self):
        try:
            latest_file = self.__get_last_file()
            if latest_file is None:
                return None
            df = pd.read_csv(latest_file)
            last_timestamp = df["open_time"].max()
            return last_timestamp
        except FileNotFoundError:
            return None

    def __format_start_time(self, timestamp):
        if timestamp is None:
            return "2017-10-10 00:00:00"  # None  # "2017-10-10 00:00:00"
        else:
            dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def __to_drop_duplicates(self, file_path):
        df = pd.read_csv(file_path)
        df.drop_duplicates(subset=["open_time"], keep="last", inplace=True)
        #
        tmp_file_path = file_path + ".tmp"
        df.to_csv(tmp_file_path, index=False)
        os.replace(tmp_file_path, file_path)

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
        # df.drop("ignore", axis=1, inplace=True)
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
            old_df = pd.read_csv(latest_file)
            old_df_len = len(old_df)

        if old_df_len != 0 and old_df_len < chunk_size:
            subset_df_len = chunk_size - old_df_len
            if subset_df_len > df_len:
                subset_df_len = df_len
            subset_df = df.iloc[:subset_df_len]
            subset_df.to_csv(latest_file, mode="a", index=False, header=False)
            self.__to_drop_duplicates(latest_file)
            start_i = int(latest_file.split("_")[-1].split(".")[0]) + 1

        log.logger.debug(
            f"{self.symbol} {self.time_frame}, latest_file: {latest_file}, df_len: {df_len} , old_df_len: {old_df_len}, subset_df_len: {subset_df_len}, start_i: {start_i} {self.ctime - time.time()}"
        )
        if subset_df_len >= df_len:
            return

        merged_data = df
        if subset_df_len > 0:
            merged_data = df.iloc[subset_df_len:]
        merged_data_len = len(merged_data)
        if merged_data_len == 0:
            return

        if chunk_size >= merged_data_len:
            file_name = f"{self.file_prefix}{start_i}.csv"
            output_file = os.path.join(self.output_dir, file_name)
            merged_data.to_csv(output_file, index=False)
            self.__to_drop_duplicates(output_file)
            log.logger.debug(
                f"{self.symbol} {self.time_frame}, start_i file_name: {file_name} {self.ctime - time.time()}"
            )
            return

        chunks = [
            chunk
            for chunk in np.array_split(
                merged_data, math.ceil(merged_data_len / chunk_size)
            )
        ]
        for i, chunk in enumerate(chunks, start=start_i):
            file_name = f"{self.file_prefix}{i}.csv"
            output_file = os.path.join(self.output_dir, file_name)
            chunk.to_csv(output_file, index=False)
            self.__to_drop_duplicates(output_file)
            log.logger.debug(
                f"{self.symbol} {self.time_frame}, i file_name: {file_name} {self.ctime - time.time()}"
            )

    #########################

    def get_history_klines(self):
        klines = self.__get_latest_klines()
        if klines:
            self.__save_klines_to_csv(klines)

    #
    def get_klines_start_end_time(self, start_time, end_time=None):
        start_str = self.__format_start_time(start_time)
        end_str = None
        if end_time is not None:
            end_str = self.__format_start_time(end_time)
        klines = self.client.get_historical_klines(
            self.symbol, self.time_frame, start_str, end_str
        )
        return klines

    #
    def run_periodic_updates(self, interval_seconds=60):
        def job():
            self.update_and_save_klines()

        schedule.every(interval_seconds).seconds.do(job)

        while True:
            schedule.run_pending()
            time.sleep(3)

    def get_now_klines(self, limit=1):
        return self.client.get_klines(
            symbol=self.symbol, interval=self.interval, limit=limit
        )
