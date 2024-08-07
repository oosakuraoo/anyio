from enum import Enum


class ORDER_OTHER_PARAM(Enum):
    TOOL_BUY_AMOUNT = 500.00
    ONCE_BUY_AMOUNT = 80.00

    ONCE_QUOTE_ORDER_MAX_LEN = 2


class QUOTE_ASSET_TYPE(Enum):
    USDT = "USDT"
    BTC = "BTC"


class STATUS(Enum):
    TRADING = "TRADING"


class TIME_FRAME(Enum):
    KLINE_INTERVAL_1MINUTE = "1m"
    KLINE_INTERVAL_3MINUTE = "3m"
    KLINE_INTERVAL_5MINUTE = "5m"
    KLINE_INTERVAL_15MINUTE = "15m"
    KLINE_INTERVAL_30MINUTE = "30m"
    KLINE_INTERVAL_1HOUR = "1h"
    # KLINE_INTERVAL_2HOUR = "2h"
    KLINE_INTERVAL_4HOUR = "4h"
    # KLINE_INTERVAL_6HOUR = "6h"
    # KLINE_INTERVAL_8HOUR = "8h"
    # KLINE_INTERVAL_12HOUR = "12h"
    KLINE_INTERVAL_1DAY = "1d"
    # KLINE_INTERVAL_3DAY = "3d"
    # KLINE_INTERVAL_1WEEK = "1w"
    # KLINE_INTERVAL_1MONTH = "1M"


class SYMBOL_TYPE(Enum):
    SPOT = "SPOT"


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


class ORDER_TYPE(Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP_LOSS = "STOP_LOSS"
    STOP_LOSS_LIMIT = "STOP_LOSS_LIMIT"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_LIMIT = "TAKE_PROFIT_LIMIT"
    LIMIT_MAKER = "LIMIT_MAKER"


class ORDER_RESP_TYPE(Enum):
    FULL = "FULL"
    RESULT = "RESULT"
    ACK = "ACK"


class TIME_IN_FORCE(Enum):
    GTC = "GTC"  # Good till cancelled
    IOC = "IOC"  # Immediate or cancel
    FOK = "FOK"  # Fill or kill


class WEBSOCKET_DEPTH(Enum):
    WEBSOCKET_DEPTH_0 = "0"
    WEBSOCKET_DEPTH_5 = "5"
    WEBSOCKET_DEPTH_10 = "10"
    WEBSOCKET_DEPTH_20 = "20"
