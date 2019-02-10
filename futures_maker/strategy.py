# 交易策略的基类
import datetime
import logging
from futures_maker.numeric import Wad


class Strategy:
    logger = logging.getLogger()

    """策略基类"""
    def __init__(self, instrument_id: str):
        self.instrument_id = instrument_id
        self.open_position_function = None
        self.close_posion_function = None
        self.cancel_orders_function = None
        self.api = None
        self.websocket_feed = None

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


class TrandStrategy(Strategy):

    """trading by trand strategy"""
    def __init__(self, instrument_id):
        super().__init__(instrument_id)
        self.logger.debug(f"init TrandStrategy")
        self.last_time = None
        self.last_price = None
        self.type_descs = {1: '开多', 2: '开空', 3: '平多', 4: '平空'}

        self.spot_ticker_last = {}
        self.swap_ticker_last = {}
        self.spot_candle60s_last = {}

        self.enter_long = False

    def match_enter_long(self):
        """1、当前1分钟线上超过0.48%"""
        if self.enter_long:
            # already open
            return False

        if 'percent' not in self.spot_candle60s_last.keys():
            return False

        if self.spot_candle60s_last['percent'] >= Wad.from_number(0.3) and \
                self.spot_candle60s_last['volume'] > Wad.from_number(2000):
            return True

        return False

    def match_exit_long(self):
        if not self.enter_long:
            return False

        enter_price, size, enter_time = self.enter_long
        gap_price = (self.swap_ticker_last['best_bid'] - enter_price)*30 / enter_price
        gap_time = datetime.datetime.now() - enter_time
        self.logger.debug(f"gap_price:{gap_price}, gap_time:{gap_time}")
        if gap_price > Wad.from_number(0.3):
            return True

        if gap_price > Wad.from_number(0.1) and gap_time > 1800:
            return True

        return False

    def match_enter_short(self):
        return False

    def match_exit_short(self):
        return False

    def run(self, item: dict):
        """处理每一个监听数据，触发执行策略"""
        self.logger.info(f"process message: {item}")
        for data in item['data']:
            if item['table'] == 'spot/ticker':
                self.spot_ticker_last = data
            elif item['table'] == 'swap/ticker':
                self.swap_ticker_last = data
            elif item['table'] == 'spot/candle60s':
                candle = data['candle']
                self.spot_candle60s_last['timestamp'] = candle[0]
                self.spot_candle60s_last['open'] = Wad.from_number(candle[1])
                self.spot_candle60s_last['high'] = Wad.from_number(candle[2])
                self.spot_candle60s_last['low'] = Wad.from_number(candle[3])
                self.spot_candle60s_last['close'] = Wad.from_number(candle[4])
                self.spot_candle60s_last['volume'] = Wad.from_number(candle[5])
                self.spot_candle60s_last['percent'] = (self.spot_candle60s_last['close'] - self.spot_candle60s_last['open']) / self.spot_candle60s_last['open']

        self.logger.info(f"spot/ticker:{self.spot_ticker_last}\n"
                         f"swap/ticker:{self.swap_ticker_last}\n"
                         f"spot/candle60s: {self.spot_candle60s_last}\n")

        if self.match_enter_long():
            '''发出开多指令-1'''
            price = self.swap_ticker_last['last']
            size = 10
            order_id = self.api.place_order(self.instrument_id, 1, price, size)
            if order_id:
                self.enter_long = (price, size, datetime.datetime.now())

        if self.match_exit_long():
            '''发出平多指令-3'''
            enter_price, size, enter_time = self.enter_long
            exit_price = self.swap_ticker_last['best_bid']
            order_id = self.api.place_order(self.instrument_id, 3, exit_price, size)
            if order_id:
                self.enter_long = False


