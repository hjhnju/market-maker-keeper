# Quant Framework
#

from futures_maker.okex_api import OKExSwapApi, OKExSwapApiFactory
from futures_maker.okex_websocket_api import OKExWebsocketApi
from futures_maker.strategy import TrandStrategy, Strategy
from pymaker.lifecycle import Lifecycle

import json
import logging


class OKExFuturesMaker:

    logger = logging.getLogger()

    def __init__(self, strategy: Strategy):
        self.setup_logging(debug=True)

        with open("futures.json", "r") as f:
            self.config = json.load(f)

        logging.info(f"config {self.config}")
        self.okex_api = OKExSwapApiFactory.get_okex_swap_api(self.config)
        self.okex_websocket_api = OKExWebsocketApi(self.config["OKEX_WEBSOCKET_URL"])
        self.strategy = strategy

    def setup_logging(self, debug: bool=False):
        logging.basicConfig(format='%(asctime)-15s %(levelname)-8s %(message)s',
                                level=(logging.DEBUG if debug else logging.INFO))
        logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)
        logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.INFO)

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
    maker = OKExFuturesMaker(TrandStrategy())
    maker.main()
