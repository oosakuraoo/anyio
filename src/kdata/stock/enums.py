from enum import Enum


class ORDER_OTHER_PARAM(Enum):
    TOOL_BUY_NUM = 1000
    ONCE_BUY_NUM = 100

    ONCE_QUOTE_ORDER_MAX_LEN = 2


#  Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max


class PERIOD(Enum):
    DAILY = "1d"
    WEEKLY = "1w"
    MONTHLY = "1mo"


class TIME_FRAME(Enum):
    KLINE_INTERVAL_1MINUTE = "1m"
    KLINE_INTERVAL_5MINUTE = "5m"
    KLINE_INTERVAL_15MINUTE = "15m"
    KLINE_INTERVAL_30MINUTE = "30m"
    KLINE_INTERVAL_60MINUTE = "60m"
    KLINE_INTERVAL_1DAY = "1d"
    # KLINE_INTERVAL_5DAY = "5d"
    # KLINE_INTERVAL_1WEEK = "1wk"
    # KLINE_INTERVAL_1MONTH = "1mo"


class ORDER_STATUS(Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    PENDING_CANCEL = "PENDING_CANCEL"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class ORDER_SIDE(Enum):
    BUY = "BUY"
    SELL = "SELL"


class BALANCE_RECORD(Enum):
    IN = "IN"
    OUT = "OUT"
