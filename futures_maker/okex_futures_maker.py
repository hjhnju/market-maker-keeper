# Quant Framework
#

from futures_maker.okex_api import OKExSwapApiFactory
from futures_maker.okex_websocket_feed import OkexWebSocketFeed
from futures_maker.strategy import TrandStrategy
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

        parser.add_argument("--spot", type=str, required=True,
                            help="Token pair (sell/buy) on which the keeper will operate")

        parser.add_argument("--config", type=str, required=True,
                            help="configuration file")

        parser.add_argument("--strategy", type=str, required=True,
                            help="strategy class")

        parser.add_argument("--debug", dest='debug', action='store_true',
                            help="Enable debug output")

        self.arguments = parser.parse_args(args)
        self.instrument_id = self.arguments.pair
        self.spot_instrument_id = self.arguments.spot
        setup_logging(self.arguments)

        with open(self.arguments.config, "r") as f:
            self.config = json.load(f)

        logging.info(f"Arguments {self.arguments}, config {self.config}")
        self.okex_api = OKExSwapApiFactory.get_okex_swap_api(self.config)

        open_message_obj = {
            "op": "subscribe",
            "args": [f"swap/ticker:{self.instrument_id}",
                     f"spot/ticker:{self.spot_instrument_id}",
                     f"spot/candle60s:{self.spot_instrument_id}",
                     f"spot/candle300s:{self.spot_instrument_id}",
                     f"spot/candle900s:{self.spot_instrument_id}"]}
        open_message = str(open_message_obj)
        open_message = open_message.replace("'", '"')
        logging.info(f"send subscribe {open_message}")
        self.okex_websocket_feed = OkexWebSocketFeed(self.config["OKEX_WEBSOCKET_URL"], open_message)

        self.strategy = TrandStrategy(self.instrument_id)
        self.strategy.set_api(self.okex_api)

        self.okex_websocket_feed.set_callback(self.strategy.run)

    def sync(self):
        # do nothing
        pass

    def shutdown(self):
        logging.info(f"shutdown")
        pass

    def main(self):
        with Lifecycle() as lifecycle:
            lifecycle.initial_delay(10)
            lifecycle.every(15, self.sync)
            lifecycle.on_shutdown(self.shutdown)


if __name__ == '__main__':
    maker = OKExFuturesMaker(sys.argv[1:])
    maker.main()
