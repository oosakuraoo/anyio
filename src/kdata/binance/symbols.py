from binance.client import Client
import pandas as pd
import os
import conf.binance.klines as coin_config
from src.kdata.binance.enums import STATUS, QUOTE_ASSET_TYPE


class BinanceSymbols:
    def __init__(self):
        # 使用环境变量获取API密钥，提高安全性
        self.api_key = coin_config.api_key
        self.api_secret = coin_config.api_secret
        if not self.api_key or not self.api_secret:
            raise ValueError(
                "BINANCE_API_KEY and BINANCE_API_SECRET environment variables must be set."
            )

        self.client = Client(self.api_key, self.api_secret)

    def get_binance_usdt_pairs(self):
        exchange_info = self.client.get_exchange_info()

        usdt_pairs = []
        for symbol_info in exchange_info["symbols"]:
            quote_asset = symbol_info["quoteAsset"]
            status = symbol_info["status"]
            if (
                quote_asset == QUOTE_ASSET_TYPE.USDT.value
                and status == STATUS.TRADING.value
            ):
                usdt_pairs.append(
                    {
                        "symbol": symbol_info["symbol"],
                        "base_asset": symbol_info["baseAsset"],
                        "quote_asset": quote_asset,
                    }
                )

        return usdt_pairs
