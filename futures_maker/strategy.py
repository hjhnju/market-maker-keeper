# 交易策略的基类
import logging
import datetime

from futures_maker.time_data_queue import TimeSeriesData


class Strategy:
    logger = logging.getLogger()

    """策略基类"""
    def __init__(self, instrument_id: str):
        self.instrument_id = instrument_id
        self.open_position_function = None
        self.close_posion_function = None
        self.cancel_orders_function = None
        self.time_series_data = TimeSeriesData(instrument_id, 100)

        pass

    def set_open_position_function(self, open_position_function):
        self.open_position_function = open_position_function

    def set_close_position_function(self, close_position_function):
        self.close_posion_function = close_position_function

    def set_cancel_orders_function(self, cancel_orders_function):
        self.cancel_orders_function = cancel_orders_function

    def run(self, item: dict):
        """处理每一个监听数据，触发执行策略"""
        self.logger.info(f"run strategy by trigger {item}")
        self.store_price(item)
        self.cancel_unfill_orders()

        if self.match_open_position(item):
            '''发出开仓指令'''
            if callable(self.open_position_function):
                open_price = None
                open_amount = None
                self.open_position_function(open_price, open_amount)

        if self.match_close_position(item):
            '''发出平仓指令'''
            if callable(self.close_posion_function):
                order_id = None
                close_amount = None
                close_price = None
                self.close_posion_function(order_id, close_price, close_amount)

    def store_price(self, item):
        self.time_series_data.push(item)

    def cancel_unfill_orders(self):
        """未成交开仓订单取消"""
        if callable(self.cancel_orders_function):
            self.cancel_orders_function()

    def match_open_position(self, item: dict):
        """开仓策略"""
        raise NotImplemented()

    def match_close_position(self, item: dict) -> bool:
        """平仓策略"""
        raise NotImplemented()


class TrandStrategy(Strategy):

    long_positions = []
    short_positions = []

    """trading by trand strategy"""
    def __init__(self):
        super().__init__()
        self.logger.debug(f"init TrandStrategy")
        self.last_time = None
        self.last_price = None

    def match_open_position(self, item: dict):
        """1、当前1分钟线下跌超过0.48%"""
        return False

    def is_full_long_positions(self):
        if len(self.long_positions) >= 2:
            return True
        return False

    def is_full_short_positions(self):
        if len(self.short_positions) >= 2:
            return True
        return False



