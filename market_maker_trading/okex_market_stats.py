
import argparse
import logging
import sys
import numpy as np
import time

from retry import retry

from market_maker_keeper.limit import History
from market_maker_keeper.order_book import OrderBookManager
from market_maker_keeper.order_history_reporter import create_order_history_reporter
from market_maker_keeper.price_feed import PriceFeedFactory
from market_maker_keeper.reloadable_config import ReloadableConfig
from market_maker_keeper.util import setup_logging
from pyexchange.okex import OKEXApi, Order
from pymaker.numeric import Wad


"""Trading for Volume"""
class OkexMarketStats:
    """Keeper acting as a market maker on OKEX."""

    logger = logging.getLogger()

    def __init__(self, args: list):
        parser = argparse.ArgumentParser(prog='okex-market-trading')

        parser.add_argument("--okex-api-server", type=str, default="https://www.okex.com",
                            help="Address of the OKEX API server (default: 'https://www.okex.com')")

        parser.add_argument("--okex-api-key", type=str, required=True,
                            help="API key for the OKEX API")

        parser.add_argument("--okex-secret-key", type=str, required=True,
                            help="Secret key for the OKEX API")

        parser.add_argument("--okex-passphrase", type=str, required=True,
                            help="Passphrase for the OKEX API")

        parser.add_argument("--okex-timeout", type=float, default=9.5,
                            help="Timeout for accessing the OKEX API (in seconds, default: 9.5)")

        parser.add_argument("--pair", type=str, required=True,
                            help="Token pair (sell/buy) on which the keeper will operate")

        parser.add_argument("--config", type=str, required=True,
                            help="Bands configuration file")

        parser.add_argument("--price-feed", type=str, required=True,
                            help="Source of price feed")

        parser.add_argument("--price-feed-expiry", type=int, default=120,
                            help="Maximum age of the price feed (in seconds, default: 120)")

        parser.add_argument("--spread-feed", type=str,
                            help="Source of spread feed")

        parser.add_argument("--spread-feed-expiry", type=int, default=3600,
                            help="Maximum age of the spread feed (in seconds, default: 3600)")

        parser.add_argument("--control-feed", type=str,
                            help="Source of control feed")

        parser.add_argument("--control-feed-expiry", type=int, default=86400,
                            help="Maximum age of the control feed (in seconds, default: 86400)")

        parser.add_argument("--order-history", type=str,
                            help="Endpoint to report active orders to")

        parser.add_argument("--order-history-every", type=int, default=30,
                            help="Frequency of reporting active orders (in seconds, default: 30)")

        parser.add_argument("--refresh-frequency", type=int, default=3,
                            help="Order book refresh frequency (in seconds, default: 3)")

        parser.add_argument("--debug", dest='debug', action='store_true',
                            help="Enable debug output")

        self.arguments = parser.parse_args(args)
        setup_logging(self.arguments)

        self.bands_config = ReloadableConfig(self.arguments.config)
        self.price_feed = PriceFeedFactory().create_price_feed(self.arguments)

        self.order_history_reporter = create_order_history_reporter(self.arguments)

        self.history = History()
        self.okex_api = OKEXApi(api_server=self.arguments.okex_api_server,
                                api_key=self.arguments.okex_api_key,
                                secret_key=self.arguments.okex_secret_key,
                                passphrase=self.arguments.okex_passphrase,
                                timeout=self.arguments.okex_timeout)

        self.order_book_manager = OrderBookManager(refresh_frequency=self.arguments.refresh_frequency)
        self.order_book_manager.get_orders_with(lambda: self.okex_api.get_orders(self.pair()))
        self.order_book_manager.get_balances_with(lambda: self.okex_api.get_balances())
        self.order_book_manager.cancel_orders_with(lambda order: self.okex_api.cancel_order(self.pair(), order.order_id))
        self.order_book_manager.enable_history_reporting(self.order_history_reporter, self.our_buy_orders, self.our_sell_orders)
        self.order_book_manager.start()

    def main(self):
        balances = self.okex_api.get_balances()
        self.logger.info(f"Balances: {balances}")

        self.order_book_manager.cancel_all_orders()
        self.logger.info(f"Refresh balances: {balances}")

    def shutdown(self):
        self.order_book_manager.cancel_all_orders()

    def pair(self):
        return self.arguments.pair.upper()

    def token_sell(self) -> str:
        return self.arguments.pair.split('-')[0].upper()

    def token_buy(self) -> str:
        return self.arguments.pair.split('-')[1].upper()

    def our_available_balance(self, our_balances: list, token: str) -> Wad:
        for item in our_balances:
            if token == item['currency']:
                return Wad.from_number(item['available'])
        return Wad(0)

    def our_sell_orders(self, our_orders: list) -> list:
        return list(filter(lambda order: order.is_sell, our_orders))

    def our_buy_orders(self, our_orders: list) -> list:
        return list(filter(lambda order: not order.is_sell, our_orders))

if __name__ == '__main__':
    OkexMarketStats(sys.argv[1:]).main()
