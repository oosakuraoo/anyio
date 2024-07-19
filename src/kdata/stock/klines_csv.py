import yfinance as yf
import pandas as pd
import numpy as np
import math
import conf.stock.config as other_config
import conf.config as config
import os
import glob
from natsort import natsorted
from datetime import datetime, timedelta, timezone
import time
from src.kdata.stock.enums import TIME_FRAME as STOCK_TIME_FRAME
from src.lib.comm.util import DateUtil
from src.lib.log import Logger, LogLevel

log = Logger()


class SymbolKlinesToCSV:
    def __init__(self, symbol, time_frame):
        self.symbol = symbol
        self.time_frame = time_frame
        self.period = "max"
        self.output_dir = config.stock_output_dir + symbol
        self.file_prefix = f"{self.symbol}_{self.time_frame}_"
        self.ctime = time.time()

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
            return None
        else:
            dt = int(timestamp / 1000)
            return dt

    def __to_drop_duplicates(self, file_path):
        df = pd.read_csv(file_path)
        df.drop_duplicates(subset=["open_time"], keep="last", inplace=True)
        #
        tmp_file_path = file_path + ".tmp"
        df.to_csv(tmp_file_path, index=False)
        os.replace(tmp_file_path, file_path)

    def __download_klines(self, start_second):
        if STOCK_TIME_FRAME.KLINE_INTERVAL_1DAY.value == self.time_frame:
            print(f"start_second: {self.symbol} {self.time_frame} {start_second}")
            return yf.download(
                self.symbol,
                period=self.period,
                interval=self.time_frame,
                start=start_second,
                threads=True,
                show_errors=False,
            )

        #
        # self.period = "1mo" if self.time_frame in "m" else "max"
        data = None
        try:
            if start_second is None:
                ticker = yf.Ticker(self.symbol)
                info = ticker.info
                # print(info)
                start_second = info.get("firstTradeDateEpochUtc")
                # 2019-12-12 00:00:00 1576080000 , 2020-12-12 00:00:00 1607702400
                if start_second < 1607702400:
                    start_second = 1607702400
            print(f"start_second: {self.symbol} {self.time_frame} {start_second}")
            run_state = True
            i = 0
            while run_state:
                end_second = start_second + 86400 * 55
                end_second = end_second if end_second < time.time() else None
                print(f"{i} end_second: {self.symbol} {self.time_frame} {end_second}")
                # df = ticker.history(
                #     period=self.period,
                #     interval=self.time_frame,
                #     start=start_second,
                #     end=end_second,
                # )
                df = yf.download(
                    self.symbol,
                    period=self.period,
                    interval=self.time_frame,
                    start=start_second,
                    end=end_second,
                    threads=True,
                    show_errors=False,
                )
                print(
                    f"{self.symbol} {self.time_frame} {self.period} {start_second} {end_second}"
                )
                if data is None:
                    data = df
                else:
                    data = pd.concat([data, df])
                print("data", i, len(data))
                start_second = end_second
                i += 1
                if end_second is None:
                    run_state = False
                    print(f"run_state: {run_state}")
            time.sleep(1)
        except Exception as e:
            print(
                f"__download_klines err: {e} {self.symbol} {self.time_frame} {self.period} {start_second} {end_second}"
            )
        return data

    def __get_latest_klines(self):
        latest_timestamp = self.__get_last_saved_timestamp()
        start_second = self.__format_start_time(latest_timestamp)
        return self.__download_klines(start_second)

    def __save_klines_to_csv(self, klines):
        df_cp = klines.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }
        )
        df = df_cp.copy()
        df["open_time"] = None
        for val in df.index:
            df.loc[val, "open_time"] = DateUtil.str_to_millisecond(
                val.strftime("%Y-%m-%d %H:%M:%S")
            )
        df["close_time"] = df["open_time"] + (
            DateUtil.switch_timeframe_to_millisecond(self.time_frame) - 1
        )
        # df = df.reset_index(drop=True)
        # print(df)
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
        print(f"#### get_history_klines {self.symbol} {self.time_frame} success")
        if klines is None:
            return
        if klines.empty:
            return
        self.__save_klines_to_csv(klines)
