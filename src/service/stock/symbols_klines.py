import sys
import time
import conf.table as table_config
from src.lib.log import Logger
from src.lib.mongo.db import MongoDBManager
from src.kdata.stock.klines_csv import SymbolKlinesToCSV
from src.kdata.stock.symbols import DFCFSymbols, XinLangSymbols, THSSymbols
from src.lib.technical.filter import StockFilter
import conf.config as config

log = Logger()

stockFilter = StockFilter()


# @log.logger.p
class SymbolsKlines:
    def __init__(self):
        self.type = "dfcf"
        self.symbols_table_name, self.SymbolsClass = self.__switch_case(self.type)

    def __switch_case(self, case):
        switch_dict = {
            "dfcf": (table_config.table_dfcf_symbols, DFCFSymbols()),
            "xinlang": (table_config.table_xinlang_symbols, XinLangSymbols()),
            "ths": (table_config.table_ths_symbols, THSSymbols()),
        }
        return switch_dict.get(case, (None, None))

    def add_symbols(self):
        try:
            usdt_pairs = self.SymbolsClass.get_symbols()
            if len(usdt_pairs) == 0:
                sys.exit("add_symbols fail: usdt_pairs is empty")

            mongo = MongoDBManager()
            try:
                with mongo.connect() as db_obj:
                    mongo.delete_many(self.symbols_table_name, {})
                    mongo.insert_many(self.symbols_table_name, usdt_pairs)
                log.logger.info(f"add_symbols success: len = {len(usdt_pairs)}")
            except Exception as e:
                log.logger.error(e)
        except Exception as e:
            log.logger.error(e)

    def modify_symbols_true(self, symbol_nos):
        try:
            if symbol_nos is None or len(symbol_nos) == 0:
                return

            mongo = MongoDBManager()
            try:
                with mongo.connect() as db_obj:
                    mongo.update_many(
                        self.symbols_table_name,
                        {"symbol": {"$in": symbol_nos}},
                        {"is_true": 1},
                    )
                print(f"update_symbols success")
            except Exception as e:
                log.logger.error(e)
        except Exception as e:
            log.logger.error(e)

    def get_symbols(self):
        try:
            mongo = MongoDBManager()
            with mongo.connect() as query_obj:
                symbol_list = mongo.find_list(self.symbols_table_name, {})
                print(f"symbols len: {len(symbol_list)}")
            return symbol_list
        except Exception as e:
            log.logger.error(e)
            return []

    def get_symbol_nos(self, symbols=None):
        if symbols is None:
            symbols = self.get_symbols()
        return [stockFilter.get_no(symbol) for symbol in symbols]

    def get_history_klines(self, symbol, interval):
        try:
            SymbolKlinesToCSV(
                symbol=symbol,
                time_frame=interval,
            ).get_history_klines()
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
                # test
                if config.is_test:
                    break
        except (KeyboardInterrupt, SystemExit) as e:
            log.logger.error(e)
        except Exception as e:
            log.logger.error(e)
