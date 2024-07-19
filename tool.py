import conf.config as config
from src.service.binance.symbols_klines import SymbolsKlines as CoinSymbolsKlines
from src.service.stock.symbols_klines import SymbolsKlines as StockSymbolsKlines
from src.kdata.binance.enums import TIME_FRAME as COIN_TIME_FRAME
from src.kdata.stock.enums import TIME_FRAME as STOCK_TIME_FRAME
from src.lib.comm.util import DateUtil
import glob
import os
import pandas as pd
import argparse
from natsort import natsorted
import sys

# print(sys.path)

type = "coin"


def get_symbols():
    # if type == "stock":
    #     return StockSymbolsKlines().get_symbol_nos()
    return CoinSymbolsKlines().get_symbols(onlyName=True)


def get_outpath():
    # if type == "stock":
    #     return config.stock_output_dir
    return config.coin_output_dir


def get_timeframes():
    # if type == "stock":
    #     return [
    #         STOCK_TIME_FRAME.KLINE_INTERVAL_1MINUTE.value,
    #         STOCK_TIME_FRAME.KLINE_INTERVAL_5MINUTE.value,
    #         STOCK_TIME_FRAME.KLINE_INTERVAL_15MINUTE.value,
    #         STOCK_TIME_FRAME.KLINE_INTERVAL_30MINUTE.value,
    #         STOCK_TIME_FRAME.KLINE_INTERVAL_60MINUTE.value,
    #         STOCK_TIME_FRAME.KLINE_INTERVAL_1DAY.value,
    #     ]
    return [
        COIN_TIME_FRAME.KLINE_INTERVAL_1MINUTE.value,
        COIN_TIME_FRAME.KLINE_INTERVAL_3MINUTE.value,
        COIN_TIME_FRAME.KLINE_INTERVAL_5MINUTE.value,
        COIN_TIME_FRAME.KLINE_INTERVAL_15MINUTE.value,
        COIN_TIME_FRAME.KLINE_INTERVAL_30MINUTE.value,
        COIN_TIME_FRAME.KLINE_INTERVAL_1HOUR.value,
        COIN_TIME_FRAME.KLINE_INTERVAL_4HOUR.value,
        COIN_TIME_FRAME.KLINE_INTERVAL_1DAY.value,
    ]


def get_numeric_part(file_name):
    parts = file_name.split(".")
    parts = parts[0].split("_")
    n = parts[-1]
    return int(n) if n.isdigit() else 0


############################################


# 删除重复数据
def read_and_dedup(symbol_i, symbol, time_frame):
    output_dir = get_outpath() + symbol
    file_prefix = f"{symbol}_{time_frame}_"
    file_pattern = f"{file_prefix}*.csv"

    all_values = set()
    arr_files = glob.glob(os.path.join(output_dir, file_pattern))
    if len(arr_files) == 0:
        return
    sorted_csv_files = natsorted(arr_files)
    last_file = sorted_csv_files[-1]
    is_remove = False
    for file_path in sorted_csv_files:
        if os.path.getsize(file_path) == 0:
            continue
        df = pd.read_csv(file_path)
        removes = []
        for line in df["close_time"]:
            if line in all_values:
                removes.append(line)
            all_values.add(line)
        removes_len = len(removes)
        if removes_len > 0:
            df.drop_duplicates(subset=["close_time"], keep="last", inplace=True)
            # df.to_csv(file_path, index=False)
            tmp_file_path = file_path + ".tmp"
            df.to_csv(tmp_file_path, index=False)
            os.replace(tmp_file_path, file_path)
            print(
                f"end {symbol_i} {symbol} {time_frame} {file_path} {removes_len} {removes[0]} {removes[removes_len-1]} {last_file}"
            )
            is_remove = True
    # 删掉所有文件重新获取
    if is_remove:
        for file_path in sorted_csv_files:
            os.remove(file_path)


# 检查数据是否连贯并补齐
def check_lianguan(symbol_i, symbol, time_frame):
    output_dir = get_outpath() + symbol
    file_prefix = f"{symbol}_{time_frame}_"
    file_pattern = f"{file_prefix}*.csv"

    arr_files = glob.glob(os.path.join(output_dir, file_pattern))
    if len(arr_files) == 0:
        return
    sorted_csv_files = natsorted(arr_files)
    last_file = sorted_csv_files[-1]
    for file_path in sorted_csv_files:
        if os.path.getsize(file_path) == 0:
            continue
        df = pd.read_csv(file_path)
        values = {}
        starttime = None
        endtime = None
        lianguan = True
        index = 0
        i = 0
        for line in df["open_time"]:
            endtime = line
            if starttime == endtime and lianguan == False:
                if i > 1:
                    values[index]["starttime"] = df["open_time"][i - 2]
                lianguan = True
                index += 1
            if starttime != None and starttime != endtime:
                values[index] = {}
                values[index]["endtime"] = line
                values[index]["file"] = file_path
                if i > 0:
                    values[index]["starttime"] = df["open_time"][i - 1]
                lianguan = False
            starttime = line + DateUtil.switch_timeframe_to_millisecond(time_frame)
            i += 1
        if len(values) > 0:
            # print(f"end {symbol_i} {symbol} {time_frame} {values}")
            for k in values:
                lines_len = None
                # print(f". {symbol_i} {symbol} {time_frame} {k} {values[k]}")
                if values[k]["starttime"] >= 1704038400000:
                    # lines = SymbolsKlines().get_klines_start_end_time(
                    #     symbol, time_frame, values[k]["starttime"], values[k]["endtime"]
                    # )
                    # lines_len = len(lines)
                    print(
                        f"- {symbol_i} {symbol} {time_frame} {k} {values[k]} {lines_len} {last_file}"
                    )
                    if last_file == values[k]["file"]:
                        os.remove(values[k]["file"])


############################################


# 检查数据
def check_datas():
    symbols = get_symbols()
    time_frames = get_timeframes()
    symbol_i = 0
    for symbol in symbols:
        symbol_i += 1
        # if symbol_i < 40:
        #     continue
        for time_frame in time_frames:
            # print(f"-- {symbol_i} {symbol} {time_frame}")
            check_lianguan(symbol_i, symbol, time_frame)
        print(f"out {symbol_i} {symbol}")
    print("end----------------------------------------")


# 去重
def remove_duplicates():
    symbols = get_symbols()
    time_frames = get_timeframes()
    symbol_i = 0
    for symbol in symbols:
        symbol_i += 1
        # if symbol_i < 18:
        #     continue
        for time_frame in time_frames:
            # print(f"-- {symbol_i} {symbol} {time_frame}")
            read_and_dedup(symbol_i, symbol, time_frame)
        print(f"out {symbol_i} {symbol}")
    print("end----------------------------------------")


############################################


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="check")
    parser.add_argument("--dedup", action="store_true", help="dedup")

    args = parser.parse_args()

    if args.check:
        # 检查是否连续数据
        check_datas()
    elif args.dedup:
        # 删除重复数据
        remove_duplicates()
    else:
        print("No task specified")


if __name__ == "__main__":
    main()

# python tool.py --check
# python tool.py --dedup
