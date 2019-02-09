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

    def match_open_position(self):
        """1、当前1分钟线下跌超过0.48%"""
        return False

    def run(self, item: dict):
        """处理每一个监听数据，触发执行策略"""
        self.logger.info(f"process message: {item}")

        if self.match_open_position():
            '''发出开多指令-1'''
            price = Wad(0)
            size = 100
            self.api.place_order(self.instrument_id, 1, price, size)

        if self.match_close_position():
            '''发出平多指令-3'''
            price = Wad(0)
            size = 100
            self.api.place_order(self.instrument_id, 3, price, size)

    def match_close_position(self) -> bool:
        """平仓策略"""
        return True

