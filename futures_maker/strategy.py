# 交易策略的基类
import logging


class Strategy:
    logger = logging.getLogger()

    """策略基类"""
    def __init__(self):
        pass

    def run(self, item: dict):
        """处理每一个监听数据，触发执行策略"""
        self.logger.info(f"run strategy by trigger {item}")
        pass

    def open_position(self, item: dict):
        """开仓策略"""
        raise NotImplemented()

    def close_position(self, item: dict) -> bool:
        """平仓策略"""
        raise NotImplemented()


class TrandStrategy(Strategy):
    """trading by trand strategy"""
    def __init__(self):
        super().__init__()


