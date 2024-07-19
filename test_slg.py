import numpy as np
from natsort import natsorted
from src.lib.technical.tool import Tool
from src.service.binance.symbols_klines import SymbolsKlines
from src.lib.technical.slg import (
    CustomSlgOne,
    CustomSlgTwo,
    CustomSlgThree,
    CustomSlgFour,
    CustomSlgFive,
)
from src.lib.redis.db import RedisClient
from src.lib.comm.util import DateUtil
from src.lib.mongo.db import MongoDBManager
import conf.table as table_config
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# from matplotlib.widgets import S


ktool = Tool()

# customSlgOne = CustomSlgOne(test=True)
# customSlgTwo = CustomSlgTwo(test=True)
# customSlgThree = CustomSlgThree(test=True)
# customSlgFour = CustomSlgFour(test=True)
customSlgFive = CustomSlgFive(test=True)

# redisDB = RedisClient()
mongoDB = MongoDBManager()
# symbols = SymbolsKlines().get_symbols(onlyName=True)

array_list = []

run = True
if run:
    try:
        with mongoDB.connect() as db_obj:
            mongoDB.delete_many(table_config.table_binance_balance + "_test", {})
            mongoDB.delete_many(table_config.table_binance_balance_record + "_test", {})
            mongoDB.delete_many(table_config.table_binance_orders + "_test", {})

    except Exception as e:
        print(f"delete test table error: {e}")


############################################################
############################################################
############################################################

# def customSlgTwo_get_1h_to(symbol, df_4h, df_15m, start_time, end_time):
#     name = f"{symbol}"
#     symbols_4h = customSlgTwo.get_4h_symbols([symbol], {name: df_4h})
#     if len(symbols_4h) == 0:
#         return

#     starti = 0
#     try:
#         starti = df_15m.index[df_15m["close_time"] > start_time].tolist()[0]
#     except Exception as e:
#         starti = df_15m.index[df_15m["close_time"] <= start_time].tolist()[-1]
#     finally:
#         if starti == 0:
#             return
#     start_time_15m = df_15m["close_time"].iloc[starti]
#     # print(
#     #     "---- start_time_15m {} {} {} {} {} {}".format(
#     #         symbol,
#     #         start_time,
#     #         DateUtil.millisecond_to_str(start_time),
#     #         starti,
#     #         start_time_15m,
#     #         DateUtil.millisecond_to_str(start_time_15m),
#     #     )
#     # )

#     endi = 0
#     try:
#         endi = df_15m.index[df_15m["close_time"] > end_time].tolist()[0]
#     except Exception as e:
#         endi = df_15m.index[df_15m["close_time"] <= end_time].tolist()[-1]
#     finally:
#         if endi == 0:
#             return
#     end_time_15m = df_15m["close_time"].iloc[endi]
#     # print(
#     #     "---- end_time_15m {} {} {} {} {} {}".format(
#     #         symbol,
#     #         end_time,
#     #         DateUtil.millisecond_to_str(end_time),
#     #         endi,
#     #         end_time_15m,
#     #         DateUtil.millisecond_to_str(end_time_15m),
#     #     )
#     # )

#     array_list.append(
#         {
#             "start_time": start_time,
#             "end_time": end_time,
#             "start_time_15m": start_time_15m,
#             "end_time_15m": end_time_15m,
#         }
#     )

#     for i in range(starti, endi):
#         customSlgTwo.to_15m_symbols([symbol], {name: df_15m.iloc[: i + 1]})


# # -------------


# def customSlgTwo_to_4h():

#     # symbols_len = len(symbols)
#     # chunk_size = symbols_len // 50
#     # symbols_parts = [
#     #     symbols[i : i + chunk_size] for i in range(0, symbols_len, chunk_size)
#     # ]
#     # print(len(symbols_parts))

#     symbols = ["BTCUSDT"]
#     for symbol in symbols:
#         df_4h = customSlgTwo.get_df_4h(symbol)
#         df_15m = customSlgTwo.get_df_15m(symbol)
#         #
#         starti = 100
#         df_15m_len = len(df_15m)
#         if df_15m_len < starti:
#             continue
#         df_4h_len = len(df_4h)
#         if df_4h_len < starti:
#             continue
#         start_time_15m = df_15m["close_time"].iloc[starti]
#         # print(
#         #     "15m start_time {} {}".format(
#         #         start_time_15m,
#         #         DateUtil.millisecond_to_str(start_time_15m),
#         #     )
#         # )
#         starti = df_4h.index[df_4h["close_time"] > start_time_15m].tolist()[0]
#         start_time = df_4h["close_time"].iloc[starti]
#         # print(
#         #     "4h starti: {} {} {} {}".format(
#         #         starti,
#         #         df_4h.iloc[starti],
#         #         start_time,
#         #         DateUtil.millisecond_to_str(start_time),
#         #     )
#         # )
#         #
#         # print(
#         #     "start {} {} {} {} -- {} {} {}".format(
#         #         symbol,
#         #         starti,
#         #         start_time_15m,
#         #         DateUtil.millisecond_to_str(start_time_15m),
#         #         (df_4h_len - starti),
#         #         start_time,
#         #         DateUtil.millisecond_to_str(start_time),
#         #     )
#         # )

#         for i in range(starti, df_4h_len):
#             start_time = df_4h["close_time"].iloc[i - 1]
#             end_time = df_4h["close_time"].iloc[i]
#             # print(
#             #     "        {} {} {} {} {}".format(
#             #         i,
#             #         start_time,
#             #         DateUtil.millisecond_to_str(start_time),
#             #         end_time,
#             #         DateUtil.millisecond_to_str(end_time),
#             #     )
#             # )

#             customSlgTwo_get_1h_to(symbol, df_4h.iloc[: i + 1], df_15m, start_time, end_time)

#         print(
#             "end {} {} {}".format(
#                 symbol,
#                 end_time,
#                 DateUtil.millisecond_to_str(end_time),
#             )
#         )

#     print("------------------------------------------------")

# customSlgTwo_to_4h()


# for arr in array_list:
#     # print(
#     #     "##########4h {} {} {} {}".format(
#     #         arr["start_time"],
#     #         DateUtil.millisecond_to_str(arr["start_time"]),
#     #         arr["end_time"],
#     #         DateUtil.millisecond_to_str(arr["end_time"]),
#     #     )
#     # )
#     print(
#         "----------15m {} {} {} {}".format(
#             arr["start_time_15m"],
#             DateUtil.millisecond_to_str(arr["start_time_15m"]),
#             arr["end_time_15m"],
#             DateUtil.millisecond_to_str(arr["end_time_15m"]),
#         )
#     )


############################################################
############################################################
############################################################


# def get_i(df):
#     starti = 0
#     try:
#         starti = df.index[df["close_time"] > df].tolist()[0]
#     except Exception as e:
#         starti = df.index[df["close_time"] <= df].tolist()[-1]
#     return starti


# def to_customSlgThree_15m():

#     symbols = ["BTCUSDT"]

#     for symbol in symbols:
#         df_4h = customSlgFour.get_df(symbol, "4h")
#         df_1h = customSlgFour.get_df(symbol, "1h")
#         df_15m = customSlgFour.get_df(symbol, "15m")
#         #
#         starti = 100
#         df_15m_len = len(df_15m)
#         if df_15m_len < starti:
#             continue

#         for i in range(starti, df_15m_len):
#             _df_4h = df_4h
#             _df_1h = df_1h
#             _df_15m = {symbol: df_15m.iloc[: i + 1]}
#             customSlgFour.to_symbol([symbol], _df_4h, _df_1h, _df_15m)

#     print("------------------------------------------------")


# to_customSlgThree_15m()


############################################################
############################################################
############################################################


def test_ma_7_30():
    symbols = ["BTCUSDT"]

    for symbol in symbols:
        df_15m = customSlgFive.get_df(symbol, "1h", 2)
        starti = 100
        df_15m_len = len(df_15m)
        if df_15m_len < starti:
            continue

        for i in range(starti, df_15m_len):
            customSlgFive.to_symbol(symbol, df_15m.iloc[: i + 1])

    print("------------------------------------------------")


if run:
    test_ma_7_30()

balance_list = []
balance_record_list = []
order_list = []
try:
    with mongoDB.connect() as db_obj:
        balance_list = mongoDB.find_list(
            table_config.table_binance_balance + "_test", {}
        )
        balance_record_list = mongoDB.find_list(
            table_config.table_binance_balance_record + "_test", {}
        )
        order_list = mongoDB.find_list(table_config.table_binance_orders + "_test", {})
except Exception as e:
    print(f"delete test table error: {e}")

# amounts_data = np.array(
#     map(lambda item: (item["amounts"] + item["frozen_amounts"]), balance_record_list)
# )

price_data = [item["price"] for item in order_list]
otime_data = [item["otime"][5:-6] for item in order_list]

buy_list = [item for item in order_list if item["side"] == "BUY"]
sell_list = [item for item in order_list if item["side"] == "SELL"]

buy_price_data = [item["price"] for item in buy_list]
sell_price_data = [item["price"] for item in sell_list]

buy_otime_data = [item["otime"][5:-6] for item in buy_list]
sell_otime_data = [item["otime"][5:-6] for item in sell_list]
#
plt.plot(otime_data, price_data, "bo--")
plt.plot(buy_otime_data, buy_price_data, "r^")
plt.plot(sell_otime_data, sell_price_data, "gv")
