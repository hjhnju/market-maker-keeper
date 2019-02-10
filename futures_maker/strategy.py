# 交易策略的基类
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

    def match_enter_long(self):
        """1、当前1分钟线上超过0.48%"""
        if 'percent' not in self.spot_candle60s_last.keys():
            return False

        if self.spot_candle60s_last['percent'] >= 0.48 and self.spot_candle60s_last['volume'] > 2000:
            return True

        return False

    def match_exit_long(self):
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
                self.spot_candle60s_last['open'] = float(candle[1])
                self.spot_candle60s_last['high'] = float(candle[2])
                self.spot_candle60s_last['low'] = float(candle[3])
                self.spot_candle60s_last['close'] = float(candle[4])
                self.spot_candle60s_last['volume'] = float(candle[5])
                self.spot_candle60s_last['percent'] = (float(candle[4]) - float(candle[1]))/float(candle[1])

        self.logger.info(f"spot/ticker:{self.spot_ticker_last}\n"
                         f"swap/ticker:{self.swap_ticker_last}\n"
                         f"spot/candle60s: {self.spot_candle60s_last}\n")

        if self.match_enter_long():
            '''发出开多指令-1'''
            price = self.swap_ticker_last['best_bid']
            size = 10
            self.api.place_order(self.instrument_id, 1, price, size)

        if self.match_exit_long():
            '''发出平多指令-3'''
            price = Wad(0)
            size = 100
            self.api.place_order(self.instrument_id, 3, price, size)


