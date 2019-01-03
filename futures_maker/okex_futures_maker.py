# Quant Framework
#

from futures_maker.okex_api import OKExSwapApi, OKExSwapApiFactory
from futures_maker.okex_websocket_api import OKExWebsocketApi
from futures_maker.strategy import TrandStrategy, Strategy
from pymaker.lifecycle import Lifecycle
from market_maker_keeper.util import setup_logging

import json
import logging
import argparse
import sys


class OKExFuturesMaker:

    logger = logging.getLogger()

    def __init__(self, args: list):
        parser = argparse.ArgumentParser(prog='okex-market-trading')
        parser.add_argument("--pair", type=str, required=True,
                            help="Token pair (sell/buy) on which the keeper will operate")

        parser.add_argument("--config", type=str, required=True,
                            help="configuration file")

        parser.add_argument("--strategy", type=str, required=True,
                            help="strategy class")

        parser.add_argument("--debug", dest='debug', action='store_true',
                            help="Enable debug output")

        self.arguments = parser.parse_args(args)
        setup_logging(self.arguments)

        with open(self.arguments.config, "r") as f:
            self.config = json.load(f)

        logging.info(f"Arguments {self.arguments}, config {self.config}")
        self.okex_api = OKExSwapApiFactory.get_okex_swap_api(self.config)
        self.okex_websocket_api = OKExWebsocketApi(self.config["OKEX_WEBSOCKET_URL"])
        # self.strategy = getattr(sys.modules[__name__], self.arguments.strategy)
        if self.arguments.strategy == "TrandStrategy":
            self.strategy = TrandStrategy()
        else:
            self.strategy = Strategy()

    def sync(self):
        pass

    def shutdown(self):
        pass

    def main(self):
        with Lifecycle() as lifecycle:
            open_messages = ['{"op": "subscribe", "args": ["swap/ticker:ETH-USD-SWAP"]',
                             '{"op": "subscribe", "args": ["swap/candle60s:ETH-USD-SWAP"]}',
                             '{"op": "subscribe", "args": ["swap/candle180s:ETH-USD-SWAP"]}',
                             '{"op": "subscribe", "args": ["swap/candle300s:ETH-USD-SWAP"]}',
                             '{"op": "subscribe", "args": ["swap/candle900s:ETH-USD-SWAP"]}',
                             ]
            self.okex_websocket_api.lisen(open_messages, self.strategy.run)

            lifecycle.initial_delay(10)
            lifecycle.every(5, self.sync)
            lifecycle.on_shutdown(self.shutdown)


if __name__ == '__main__':
    maker = OKExFuturesMaker(sys.argv[1:])
    maker.main()
