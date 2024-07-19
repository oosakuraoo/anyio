import json
from flask import Blueprint, jsonify
from src.model.res.base import ObjData, PageData
from src.lib.page import Page
from src.lib.technical.tool import Tool
from src.lib.technical.slg import CustomSlgFive

tool_api = Blueprint("tool", __name__)

ktool = Tool()
customSlgFive = CustomSlgFive(test=True)


# test
@tool_api.route("/test/<symbol>/<interval>", methods=["GET"])
def test(symbol="BTCUSDT", interval="1h"):
    df_15m = customSlgFive.get_df(symbol, interval, 2)
    # starti = 100
    # df_15m_len = len(df_15m)
    # if df_15m_len < starti:
    #     return

    # for i in range(starti, df_15m_len):
    #     customSlgFive.to_symbol(symbol, df_15m.iloc[: i + 1])
    return jsonify(ObjData(data=df_15m).to_json())


# ema_rsi
@tool_api.route(
    "/ema_rsi/<symbol>/<int:page>/<interval>/<int:timeperiod>", methods=["GET"]
)
def ema_rsi(symbol="BTCUSDT", page=1, interval="1h", timeperiod=7):
    df, pageCount = Page(page).getPageAndData(symbol, interval)
    data = ktool.generate_ema_rsi(df, timeperiod)
    return jsonify(PageData(list=data.to_numpy(), page=page, count=pageCount).to_json())


# macd
@tool_api.route("/macd/<symbol>/<int:page>/<interval>", methods=["GET"])
def macd(symbol="BTCUSDT", page=1, interval="1h"):
    df, pageCount = Page(page).getPageAndData(symbol, interval)
    data = ktool.generate_macd(df)
    return jsonify(PageData(list=data.to_numpy(), page=page, count=pageCount).to_json())


# boll
@tool_api.route(
    "/boll/<symbol>/<int:page>/<interval>/<int:timeperiod>", methods=["GET"]
)
def boll(symbol="BTCUSDT", page=1, interval="1h", timeperiod=21):
    df, pageCount = Page(page).getPageAndData(symbol, interval)
    data = ktool.generate_boll(df, timeperiod)
    return jsonify(PageData(list=data.to_numpy(), page=page, count=pageCount).to_json())
