from datetime import datetime, timedelta, timezone
from src.kdata.binance.enums import TIME_FRAME as COIN_TIME_FRAME
from src.kdata.stock.enums import TIME_FRAME as STOCK_TIME_FRAME


class DateUtil:
    @staticmethod
    def millisecond_to_str(timestamp, format_str="%Y-%m-%d %H:%M:%S"):
        return datetime.fromtimestamp(timestamp / 1000).strftime(format_str)

    @staticmethod
    def second_to_str(timestamp, format_str="%Y-%m-%d %H:%M:%S"):
        return datetime.fromtimestamp(timestamp).strftime(format_str)

    @staticmethod
    def str_to_millisecond(date_str, format_str="%Y-%m-%d %H:%M:%S"):
        return int(datetime.strptime(date_str, format_str).timestamp() * 1000)

    @staticmethod
    def str_to_second(date_str, format_str="%Y-%m-%d %H:%M:%S"):
        return int(datetime.strptime(date_str, format_str).timestamp())

    @staticmethod
    def switch_timeframe_to_millisecond(time_frame):
        switch_dict = {
            COIN_TIME_FRAME.KLINE_INTERVAL_1MINUTE.value: 1,
            COIN_TIME_FRAME.KLINE_INTERVAL_3MINUTE.value: 3,
            COIN_TIME_FRAME.KLINE_INTERVAL_5MINUTE.value: 5,
            COIN_TIME_FRAME.KLINE_INTERVAL_15MINUTE.value: 15,
            COIN_TIME_FRAME.KLINE_INTERVAL_30MINUTE.value: 30,
            STOCK_TIME_FRAME.KLINE_INTERVAL_60MINUTE.value: 60,
            COIN_TIME_FRAME.KLINE_INTERVAL_1HOUR.value: 60,
            COIN_TIME_FRAME.KLINE_INTERVAL_4HOUR.value: 60 * 4,
            COIN_TIME_FRAME.KLINE_INTERVAL_1DAY.value: 60 * 24,
        }
        return switch_dict.get(time_frame, None) * 1000 * 60
