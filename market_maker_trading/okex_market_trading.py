
import argparse
import logging
import sys
import numpy as np
import time

from retry import retry

from market_maker_keeper.band import Bands, Band, NewOrder
from market_maker_keeper.control_feed import create_control_feed
from market_maker_keeper.limit import History
from market_maker_keeper.order_book import OrderBookManager
from market_maker_keeper.order_history_reporter import create_order_history_reporter
from market_maker_keeper.price_feed import PriceFeedFactory
from market_maker_keeper.reloadable_config import ReloadableConfig
from market_maker_keeper.spread_feed import create_spread_feed
from market_maker_keeper.util import setup_logging
from pyexchange.okex import OKEXApi, Order
from pymaker.lifecycle import Lifecycle
from pymaker.numeric import Wad

from functools import reduce

from market_maker_keeper.price_feed import Price


"""Trading for Volume"""
class OkexMarketTrading:
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

        # TODO://这两个参数干嘛的
        self.spread_feed = create_spread_feed(self.arguments)
        self.control_feed = create_control_feed(self.arguments)
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
        with Lifecycle() as lifecycle:
            lifecycle.initial_delay(10)
            lifecycle.every(5, self.synchronize_orders)
            lifecycle.on_shutdown(self.shutdown)

    def shutdown(self):
        self.order_book_manager.cancel_all_orders()

    def pair(self):
        return self.arguments.pair.lower()

    def token_sell(self) -> str:
        return self.arguments.pair.split('-')[0].lower()

    def token_buy(self) -> str:
        return self.arguments.pair.split('-')[1].lower()

    def our_available_balance(self, our_balances: list, token: str) -> Wad:
        for item in our_balances:
            if token in item:
                return Wad.from_number(item['available'])
        return Wad(0)

    def our_sell_orders(self, our_orders: list) -> list:
        return list(filter(lambda order: order.is_sell, our_orders))

    def our_buy_orders(self, our_orders: list) -> list:
        return list(filter(lambda order: not order.is_sell, our_orders))

    # 在买一和卖一间隔中下单自动成交
    def synchronize_orders(self):

        # 交易触发规则：随机触发。产生一个随机数，若命中概率则交易
        current_time = time.strftime("%H")
        freq_dict = {'0':5, '1':3, '2':2, '3':1, '4':1, '5':1, '6':5, '7':8, '8':10, '9':15, '10':16, '11':10,
                '12':15, '13':10, '14':15, '15':20, '16':45, '17':10, '18':15, '19':25, '20':48, '21':15, '22':10, '23':8}
        total_freq = reduce(lambda x, y: x + y, list(freq_dict.values())) * 4
        freq = freq_dict[current_time]
        do_trade = False
        hit_number = np.random.random()
        hit_range = freq / (15.0 * 60.0)
        if hit_number < hit_range:
            do_trade = True

        if not do_trade:
            logging.debug(f"Don't trading. hit_number={hit_number}, hit_range={hit_range}")
            return
        logging.info(f"[Do trading]hit_number={hit_number}, hit_range={hit_range}")

        order_book = self.order_book_manager.get_order_book()
        current_price = self.price_feed.get_price()
        if current_price.buy_price is None or current_price.sell_price is None:
            self.logger.warning("Current_price：buy_price or sell_price is None")
            return

        # Do not place new orders if order book state is not confirmed
        if order_book.orders_being_placed or order_book.orders_being_cancelled:
            self.logger.debug("Order book is in progress, not placing new orders")
            return

        # 只会使用到buy_bands的一个配置，同时应用于买和卖，买卖统一数量
        bands = Bands.read(self.bands_config, self.spread_feed, self.control_feed, self.history)
        band = bands.buy_bands[0]
        price_gap = current_price.sell_price - current_price.buy_price

        # 确定交易的数量和价格
        trade_price = current_price.buy_price + Wad.from_number(np.random.uniform(0, float(price_gap)))
        trade_amount = Wad.from_number(np.random.uniform(float(band.min_amount), float(band.max_amount)))

        # Place new orders
        new_orders = self.create_new_orders(trade_amount=trade_amount,
                                            trade_price=trade_price,
                                            our_buy_balance=self.our_available_balance(order_book.balances, self.token_buy()),
                                            our_sell_balance=self.our_available_balance(order_book.balances, self.token_sell()),
                                            band=band
                                            )
        self.place_orders(new_orders)

    def place_orders(self, new_orders):
        def place_order_function(new_order_to_be_placed):
            amount = new_order_to_be_placed.pay_amount if new_order_to_be_placed.is_sell else new_order_to_be_placed.buy_amount
            order_id = self.okex_api.place_order(pair=self.pair(),
                                                 is_sell=new_order_to_be_placed.is_sell,
                                                 price=new_order_to_be_placed.price,
                                                 amount=amount)

            return Order(order_id, 0, self.pair(), new_order_to_be_placed.is_sell, new_order_to_be_placed.price, amount, Wad(0))

        for new_order in new_orders:
            self.order_book_manager.place_order(lambda new_order=new_order: place_order_function(new_order))

    def create_new_orders(self, trade_amount: Wad, trade_price: Wad, our_buy_balance: Wad, our_sell_balance: Wad, band : Band)-> list:
        assert(isinstance(our_buy_balance, Wad))
        assert(isinstance(our_sell_balance, Wad))
        assert(isinstance(trade_price, Wad))

        # 1、构建需要创建的订单
        new_buy_orders = []
        buy_amount = trade_amount
        # pay_amount要付出的token数量, 买单时如usdt
        pay_amount = Wad.min(buy_amount * trade_price, our_buy_balance)
        if (trade_price > Wad(0)) and (pay_amount > Wad(0)) and (buy_amount > Wad(0)):
            new_buy_orders.append(NewOrder(is_sell=False,
                                           price=trade_price,
                                           amount=buy_amount,
                                           pay_amount=pay_amount,
                                           buy_amount=buy_amount,
                                           band=band,
                                           confirm_function=lambda: self.buy_limits.use_limit(time.time(), pay_amount)))
            logging.info("Trading new_buy_order, price:%s, buy_amount:%s, pay_amount:%s" % (trade_price, buy_amount, pay_amount))

        # 2、构建等量的卖出订单
        new_sell_orders = []
        # pay_amount要付出的token数量，卖单时如tokenx
        pay_amount = Wad.min(trade_amount, our_sell_balance)
        buy_amount = pay_amount * trade_price
        if (trade_price > Wad(0)) and (pay_amount > Wad(0)) and (buy_amount > Wad(0)):
            self.logger.info(f"Trading creating new sell order amount {pay_amount} with price {trade_price}")
            new_buy_orders.append(NewOrder(is_sell=True,
                                           price=trade_price,
                                           amount=pay_amount,
                                           pay_amount=pay_amount,
                                           buy_amount=buy_amount,
                                           band=band,
                                           confirm_function=lambda: self.buy_limits.use_limit(time.time(), pay_amount)))
            logging.info("Trading new_sell_order, price:%s, buy_amount:%s, pay_amount:%s" % (trade_price, buy_amount, pay_amount))

        # 先放卖单，再放买单
        return new_sell_orders + new_buy_orders

if __name__ == '__main__':
    OkexMarketTrading(sys.argv[1:]).main()
