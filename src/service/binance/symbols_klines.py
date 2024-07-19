import sys
import time
import conf.table as table_config
from src.lib.log import Logger
from src.lib.mongo.db import MongoDBManager
from src.kdata.binance.symbols import BinanceSymbols
from src.kdata.binance.klines_csv import BinanceKlinesToCSV
from src.kdata.binance.klines_h5 import BinanceKlinesToH5
import conf.config as config

log = Logger()


class SymbolsKlines:
    def __init__(self) -> None:
        self.file_type = config.coin_file_type
        self.SymbolsClass = self.switch_case_class(self.file_type)

    def switch_case_class(self, case):
        switch_dict = {
            "csv": BinanceKlinesToCSV,
            # "h5": BinanceKlinesToH5,
        }
        return switch_dict.get(case, None)

    def add_symbols(self):
        try:
            usdt_pairs = BinanceSymbols().get_binance_usdt_pairs()
            if len(usdt_pairs) == 0:
                sys.exit("add_symbols fail: usdt_pairs is empty")

            mongo = MongoDBManager()
            try:
                with mongo.connect() as db_obj:
                    mongo.delete_many(table_config.table_binance_symbols, {})
                    mongo.insert_many(table_config.table_binance_symbols, usdt_pairs)
                log.logger.info(f"add_symbols success: len: {len(usdt_pairs)}")
            except Exception as e:
                log.logger.error(e)
        except Exception as e:
            log.logger.error(e)

    def get_symbols(self, onlyName=False):
        try:
            mongo = MongoDBManager()
            with mongo.connect() as query_obj:
                symbol_list = mongo.find_list(table_config.table_binance_symbols, {})
                print(f"symbols len: {len(symbol_list)}")
            if onlyName:
                return [symbol["symbol"] for symbol in symbol_list]
            return symbol_list
        except Exception as e:
            log.logger.error(e)
            return []

    def get_history_klines(self, symbol, interval):
        try:
            self.SymbolsClass(
                symbol=symbol,
                time_frame=interval,
            ).get_history_klines()
        except (KeyboardInterrupt, SystemExit) as e:
            sys.exit(f"{symbol} {interval} exit: {e}")
        except Exception as e:
            sys.exit(f"{symbol} {interval} error exit: {e}")

    def get_klines_start_end_time(self, symbol, interval, start_time, end_time=None):
        try:
            return self.SymbolsClass(
                symbol=symbol,
                time_frame=interval,
            ).get_klines_start_end_time(start_time, end_time)
        except (KeyboardInterrupt, SystemExit) as e:
            sys.exit(f"{symbol} {interval} exit: {e}")
        except Exception as e:
            sys.exit(f"{symbol} {interval} error exit: {e}")

    # def get_klines(self, symbol, interval):
    #     try:
    #         BinanceKlines(
    #             symbol=symbol,
    #             time_frame=interval,
    #         ).get_now_klines()
    #         # print(f"#### get_klines {symbol} {interval} success")
    #     except Exception as e:
    #         sys.exit(f"{symbol} {interval} error exit: {e}")

    #########################
    # task
    #########################

    def task_init_symbols(self):
        self.add_symbols()

    def task_klines(self, symbols, interval):
        try:
            for symbol in symbols:
                self.get_history_klines(symbol, interval)
        except (KeyboardInterrupt, SystemExit) as e:
            log.logger.error(e)
        except Exception as e:
            log.logger.error(e)
