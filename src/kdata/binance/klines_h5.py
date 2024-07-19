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
from src.lib.log import Logger
from natsort import natsorted
import logging

# import fcntl
# TODO


logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)


class BinanceKlinesToH5:
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
            file_pattern = f"{self.file_prefix}*.h5"
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
            arr_files = glob.glob(os.path.join(self.output_dir, file_pattern))
            sorted_h5_files = natsorted(arr_files)
            files_len = len(sorted_h5_files)
            if files_len == 0:
                return None
            latest_file = max(
                sorted_h5_files,
                key=os.path.getctime,
            )
            if files_len > 1:
                s = pd.HDFStore(latest_file, "r")
                df = s.get("data")
                s.close()
                if len(df) < 2:
                    os.remove(latest_file)
                    return self.__get_last_file()
            return latest_file
        except FileNotFoundError:
            return None

    # def __lock_file(file):
    #     try:
    #         fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    #         return True
    #     except BlockingIOError:
    #         return False

    # def __unlock_file(file):
    #     fcntl.flock(file, fcntl.LOCK_UN)

    def __get_last_saved_timestamp(self):
        try:
            latest_file = self.__get_last_file()
            if latest_file is None:
                return None
            # if self.__lock_file(latest_file):
            #     RuntimeError(f"{latest_file} is locked")
            s = pd.HDFStore(latest_file, "r")
            df = s.get("data")
            s.close()
            last_timestamp = df["open_time"].max()
            df = df.iloc[:-1]
            df.to_h5(latest_file, index=False)
            # self.__unlock_file(latest_file)
            return last_timestamp
        except FileNotFoundError:
            return None

    def __format_start_time(self, timestamp):
        if timestamp is None:
            return "2017-10-10 00:00:00"  # None  # "2017-10-10 00:00:00"
        else:
            dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def __save_klines_to_h5(self, klines):
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
            s = pd.HDFStore(latest_file, "r")
            old_df = s.get("data")
            s.close()
            old_df_len = len(old_df)

        if old_df_len != 0 and old_df_len < chunk_size:
            subset_df_len = chunk_size - old_df_len
            if subset_df_len > df_len:
                subset_df_len = df_len
            subset_df = df.iloc[:subset_df_len]
            subset_df = subset_df.drop_duplicates(subset=["open_time"], keep="last")
            subset_df.to_h5(latest_file, mode="a", index=False, header=False)
            start_i = int(latest_file.split("_")[-1].split(".")[0]) + 1

        logging.debug(
            f"latest_file: {latest_file}, df_len: {df_len} , old_df_len: {old_df_len}, subset_df_len: {subset_df_len}, start_i: {start_i} {self.ctime - time.time()}"
        )
        if subset_df_len >= df_len:
            return

        merged_data = df
        if subset_df_len > 0:
            merged_data = df.iloc[:-subset_df_len]
        merged_data_len = len(merged_data)
        if merged_data_len == 0:
            return

        if chunk_size >= merged_data_len:
            file_name = f"{self.file_prefix}{start_i}.h5"
            output_path = os.path.join(self.output_dir, file_name)
            merged_data = merged_data.drop_duplicates(subset=["open_time"], keep="last")
            merged_data.to_h5(output_path, index=False)
            logging.debug(f"start_i file_name: {file_name} {self.ctime - time.time()}")
            return

        chunks = [
            chunk
            for chunk in np.array_split(
                merged_data, math.ceil(merged_data_len / chunk_size)
            )
        ]
        for i, chunk in enumerate(chunks, start=start_i):
            file_name = f"{self.file_prefix}{i}.h5"
            output_path = os.path.join(self.output_dir, file_name)
            chunk = chunk.drop_duplicates(subset=["open_time"], keep="last")
            chunk.to_h5(output_path, index=False)
            logging.debug(f"i file_name: {file_name} {self.ctime - time.time()}")

    def update_and_save_klines(self):
        klines = self.__get_latest_klines()
        if klines:
            self.__save_klines_to_h5(klines)

    def get_klines_start_end_time(self, start_time, end_time):
        start_str = self.__format_start_time(start_time)
        end_str = self.__format_end_time(end_time)
        klines = self.client.get_historical_klines(
            self.symbol, self.time_frame, start_str, end_str
        )
        return klines

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
