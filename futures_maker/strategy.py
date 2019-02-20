# 交易策略的基类
import datetime
import logging
from futures_maker.numeric import Wad


class Strategy:
    logger = logging.getLogger()

    """开多指令"""
    ENTER_LONG = 1
    """开空"""
    ENTER_SHORT = 2
    """平多"""
    EXIT_LONG = 3
    """平空"""
    EXIT_SHORT = 4

    """策略基类"""
    def __init__(self, instrument_id: str):
        self.instrument_id = instrument_id
        self.api = None
        self.websocket_feed = None
        # 杠杆倍数
        self.leverage = 30
        self.do_long = True
        self.do_short = False

        """可以同时一个开多一个开空 info = (price, size, time)"""
        self.is_enter_long = False
        self.enter_long_info = Wad(0), Wad(0), datetime.datetime.utcnow()

        self.is_enter_short = False
        self.enter_short_info = Wad(0), Wad(0), datetime.datetime.utcnow()

        pass

    def set_api(self, api):
        self.api = api

    def set_websocket_feed(self, websocket_feed):
        self.websocket_feed = websocket_feed

    def run(self, item: dict):
        raise NotImplementedError()

    def cancel_unfill_orders(self):
        """未成交开仓订单取消"""
        if self.api is not None:
            for order in self.api.get_orders(self.instrument_id):
                self.api.cancel_order(self.instrument_id, order.order_id)

    def load_position(self):
        """加载持仓信息"""
        if self.api is None:
            return

        timestamp = datetime.datetime.utcnow()
        is_enter_long = False
        enter_long_info = Wad(0), Wad(0), timestamp
        is_enter_short = False
        enter_short_info = Wad(0), Wad(0), timestamp

        position = self.api.position(self.instrument_id)
        margin_mode = position['margin_mode']
        for holding in position['holding']:
            side = holding['side']
            price = Wad.from_number(float(holding['avg_cost']))
            size = Wad.from_number(int(holding['position']))
            realized_pnl = Wad.from_number(float(holding['realized_pnl']))
            timestamp = datetime.datetime.strptime(holding['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
            if side == 'long':
                is_enter_long = True
                enter_long_info = price, size, timestamp
            elif side == 'short':
                is_enter_short = True
                enter_short_info = price, size, timestamp

        self.is_enter_long = is_enter_long
        self.enter_long_info = enter_long_info
        self.is_enter_short = is_enter_short
        self.enter_short_info = enter_short_info

        self.logger.debug(f"Load position  {position}, "
                          f"is_enter_long: {self.is_enter_long}, enter_long_info: {self.enter_short_info}, "
                          f"is_enter_short: {self.is_enter_short}, enter_short_info: {self.enter_short_info}")


class TrandStrategy(Strategy):

    """trading by trand strategy"""
    def __init__(self, instrument_id):
        super().__init__(instrument_id)
        self.logger.debug(f"init TrandStrategy")

        self.spot_ticker_last = {}
        self.swap_ticker_last = {}
        self.spot_candle60s_last = {}

    def match_enter_position(self):
        """1、当前现货1分钟线上涨超过0.3%且交易量超过2k，开多"""

        if 'percent' not in self.spot_candle60s_last.keys():
            return 0, Wad(0), Wad(0)

        enter_size = Wad.from_number(100)

        self.logger.debug(f"percent:{self.spot_candle60s_last['percent']}, volume: {self.spot_candle60s_last['volume']}, "
                          f"last_price:{self.swap_ticker_last['last']}, best_bid:{self.swap_ticker_last['best_bid']}, best_ask:{self.swap_ticker_last['best_ask']}"
                          f"is_enter_long:{self.is_enter_long}, is_enter_short:{self.is_enter_short}")

        if self.do_long and (not self.is_enter_long) and \
                self.spot_candle60s_last['percent'] >= Wad.from_number(0.003) and \
                self.spot_candle60s_last['volume'] >= Wad.from_number(2000):
            enter_price = self.swap_ticker_last['best_ask']
            self.logger.info(f"Match enter long. percent:{self.spot_candle60s_last['percent']}, volume: {self.spot_candle60s_last['volume']}, "
                             f"enter_price:{enter_price}, enter_size:{enter_size}")
            return Strategy.ENTER_LONG, enter_price, enter_size

        if self.do_short and (not self.is_enter_short) and \
                self.spot_candle60s_last['percent'] <= Wad.from_number(-0.003) and \
                self.spot_candle60s_last['volume'] >= Wad.from_number(2000):
            enter_price = self.swap_ticker_last['best_bid']
            self.logger.info(f"Match enter short. percent:{self.spot_candle60s_last['percent']}, volume: {self.spot_candle60s_last['volume']}, "
                             f"enter_price:{enter_price}, enter_size:{enter_size}")
            return Strategy.ENTER_SHORT, enter_price, enter_size

        # enter_long_or_short, enter_price, enter_size
        return 0, Wad(0), Wad(0)

    def match_exit_position(self):

        # check long position
        if self.is_enter_long:
            enter_price, enter_size, enter_time = self.enter_long_info
            exit_price = self.swap_ticker_last['best_bid']
            exit_size = enter_size
            gap_price_percent = (float(exit_price) - float(enter_price)) * self.leverage / float(enter_price)
            gap_time = datetime.datetime.utcnow() - enter_time

            self.logger.debug(f"Check if match exit long. gap_price_percent:{gap_price_percent}, gap_time:{gap_time.seconds}, best_bid:{exit_price}")
            if gap_price_percent >= 1.0 or (gap_price_percent >= 0.05 and gap_time.seconds >= 60) or (gap_time.seconds >= 3600):
                self.logger.info(f"Match exit long. gap_price_percent:{gap_price_percent}, gap_time:{gap_time.seconds}, exit_price:{exit_price}")
                return Strategy.EXIT_LONG, exit_price, exit_size

        # check short position
        if self.is_enter_short:
            enter_price, enter_size, enter_time = self.enter_short_info
            exit_price = self.swap_ticker_last['best_ask']
            exit_size = enter_size
            gap_price_percent = - (float(exit_price) - float(enter_price)) * self.leverage / float(enter_price)
            gap_time = datetime.datetime.utcnow() - enter_time

            self.logger.debug(f"Check if match exit short. gap_price_percent:{gap_price_percent}, gap_time:{gap_time.seconds}, best_ask:{exit_price}")
            if gap_price_percent >= 1.0 or (gap_price_percent >= 0.05 and gap_time.seconds >= 60) or (gap_time.seconds >= 3600):
                self.logger.info(f"Match exit short. gap_price_percent:{gap_price_percent}, gap_time:{gap_time.seconds}, "
                                 f"enter_price:{enter_price}, exit_price:{exit_price}")
                return Strategy.EXIT_SHORT, exit_price, exit_size

        # (exit_long_or_short, exit_price, exit_size)
        return 0, Wad(0), Wad(0)

    def run(self, item: dict):
        """处理每一个监听数据，触发执行策略"""
        self.logger.debug(f"process message: {item}")
        for data in item['data']:
            if item['table'] == 'spot/ticker':
                self.spot_ticker_last['best_ask'] = Wad.from_number(data['best_ask'])
                self.spot_ticker_last['best_bid'] = Wad.from_number(data['best_bid'])
                self.spot_ticker_last['last'] = Wad.from_number(data['last'])
            elif item['table'] == 'swap/ticker':
                self.swap_ticker_last['best_ask'] = Wad.from_number(data['best_ask'])
                self.swap_ticker_last['best_bid'] = Wad.from_number(data['best_bid'])
                self.swap_ticker_last['last'] = Wad.from_number(data['last'])
            elif item['table'] == 'spot/candle60s':
                candle = data['candle']
                self.spot_candle60s_last['timestamp'] = candle[0]
                self.spot_candle60s_last['open'] = Wad.from_number(candle[1])
                self.spot_candle60s_last['high'] = Wad.from_number(candle[2])
                self.spot_candle60s_last['low'] = Wad.from_number(candle[3])
                self.spot_candle60s_last['close'] = Wad.from_number(candle[4])
                self.spot_candle60s_last['volume'] = Wad.from_number(candle[5])
                self.spot_candle60s_last['percent'] = (self.spot_candle60s_last['close'] - self.spot_candle60s_last['open']) / self.spot_candle60s_last['open']

        # 1、check if open position
        enter_long_or_short, enter_price, enter_size = self.match_enter_position()
        timestamp = datetime.datetime.utcnow()
        if enter_long_or_short > 0 and enter_price > Wad(0) and enter_size > Wad(0):
            order_id = self.api.place_order(self.instrument_id, enter_long_or_short, enter_price, enter_size)
            if order_id:
                if enter_long_or_short == Strategy.ENTER_LONG:
                    self.enter_long_info = (enter_price, enter_size, timestamp)
                    self.is_enter_long = True
                elif enter_long_or_short == Strategy.ENTER_SHORT:
                    self.enter_short_info = (enter_price, enter_size, timestamp)
                    self.is_enter_short = True

        # 2、check if exit position
        exit_long_or_short, exit_price, exit_size = self.match_exit_position()
        if exit_long_or_short > 0 and exit_price > Wad(0) and exit_size > Wad(0):
            if enter_long_or_short == Strategy.ENTER_LONG:
                self.enter_long_info = Wad(0), Wad(0), timestamp
                self.is_enter_long = False
            elif enter_long_or_short == Strategy.ENTER_SHORT:
                self.enter_short_info = Wad(0), Wad(0), timestamp
                self.is_enter_short = False
            self.api.place_order(self.instrument_id, exit_long_or_short, exit_price, exit_size)
