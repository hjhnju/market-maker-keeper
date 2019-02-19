
from typing import List, Optional
from pprint import pformat

import logging
import requests
import hmac
import base64
import datetime
import json

from futures_maker.numeric import Wad


class Order:
    def __init__(self, order_id: str, timestamp: str, instrument_id: str,
                 type: int, price: Wad, size: Wad, filled_qty: Wad, fee: float, status: int, contract_val: float):
        assert(isinstance(order_id, str))
        assert(isinstance(timestamp, str))
        assert(isinstance(instrument_id, str))
        assert(isinstance(type, int))
        assert(isinstance(price, Wad))
        assert(isinstance(size, Wad))
        assert(isinstance(filled_qty, Wad))

        self.order_id = order_id
        self.timestamp = timestamp
        self.instrument_id = instrument_id
        self.type = type
        self.price = price
        self.size = size
        self.filled_qty = filled_qty
        self.contract_val = contract_val
        self.fee = fee
        self.status = status

    def __eq__(self, other):
        assert(isinstance(other, Order))

        return self.order_id == other.order_id and \
               self.instrument_id == other.instrument_id

    def __hash__(self):
        return hash((self.order_id, self.instrument_id))

    def __repr__(self):
        return pformat(vars(self))


class OKExSwapApi:
    """OKEx 永续合约API V3
    """

    logger = logging.getLogger()

    def __init__(self, api_server: str, api_key: str, secret_key: str, passphrase: str, timeout: int):
        assert(isinstance(api_server, str))
        assert(isinstance(api_key, str))
        assert(isinstance(secret_key, str))
        assert(isinstance(passphrase, str))
        assert(isinstance(timeout, int))

        self.api_server = api_server
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.timeout = timeout

    def ticker(self, instrument_id: str):
        """获取合约的最新成交价、买一价、卖一价和24交易量。
        GET /api/swap/v3/instruments/BTC-USD-SWAP/ticker
        """
        assert(isinstance(instrument_id, str))
        return self._http_get(f"/api/swap/v3/instruments/{instrument_id}/ticker")

    def depth(self, instrument_id: str):
        """获取合约的深度列表。
        GET /api/swap/v3/instruments/<instrument_id>/depth?size=50
        """
        assert(isinstance(instrument_id, str))
        return self._http_get(f"/api/swap/v3/instruments/{instrument_id}/depth")

    def position(self, instrument_id: str):
        """获取某个合约的持仓信息
        GET /api/swap/v3/BTC-USD-SWAP/position
        """
        assert(isinstance(instrument_id, str))
        return self._http_get(f"/api/swap/v3/{instrument_id}/position")

    def get_accounts(self, instrument_id: str) -> dict:
        """单个币种合约账户信息
        GET /api/swap/v3/BTC-USD-SWAP/accounts
        """
        assert(isinstance(instrument_id, str))
        return self._http_get(f"/api/swap/v3/{instrument_id}/accounts")

    def get_setting(self, instrument_id: str):
        """获取某个合约的杠杆倍数，持仓模式
        GET /api/swap/v3/accounts/BTC-USD-SWAP/settings
        """
        assert(isinstance(instrument_id, str))
        return self._http_get(f"/api/swap/v3/accounts/{instrument_id}/settings")

    def set_setting(self, instrument_id: str, leverage: int, side: int):
        """设定某个合约的杠杆倍数
        leverage	String	是	新杠杆倍数，可填写1-40之间的整数
        side	String	是	方向:1.逐仓-多仓 2.逐仓-空仓 3.全仓
        POST /api/swap/v3/accounts/BTC-USD-SWAP/leverage{"leverage": "10","side": "1"}
        """
        assert(isinstance(instrument_id, str))
        assert(isinstance(leverage, int))
        assert(isinstance(side, int))
        self.logger.info(f"Set_setting instrument_id #{instrument_id}(leverage={leverage}, side={side}")
        result = self._http_post(f"/api/swap/v3/accounts/{instrument_id}/leverage", {
            'leverage': str(leverage),
            'side': str(side)
        })
        self.logger.info(f"Setting result {result}")

    def get_orders(self, instrument_id: str) -> List[Order]:
        """获取active订单
        GET /api/swap/v3/orders/<instrument_id>
        GET /api/swap/v3/orders/BTC-USD-SWAP?status=2&from=4&limit=100
        status	String	是	订单状态(-2:失败 -1:撤单成功 0:等待成交 1:部分成交 2:完全成交)
        limit	String	否	分页返回的结果集数量，默认为100，最大为100，按时间倒序排列，越晚下单的在前面（按时间来排序）
        """
        assert(isinstance(instrument_id, str))

        result = self._http_get(f"/api/swap/v3/orders/{instrument_id}", 'status=0')

        self.logger.info(f"Get orders {result}")
        orders = filter(self._filter_order, result['order_info'])
        return list(map(self._parse_order, orders))

    def place_order(self, instrument_id: str, type: int, price: Wad, size: Wad) -> str:
        """下单
        type	String	是	可填参数：1:开多 2:开空 3:平多 4:平空
        match_price	String	否	是否以对手价下单 0:不是 1:是
        POST /api/swap/v3/order{"client_oid":"12233456","size":"2","type":"1","match_price":"0","price":"432.11","instrument_id":"BTC-USD-SWAP"}
        """
        assert(isinstance(instrument_id, str))
        assert(isinstance(type, int))
        assert(isinstance(price, Wad))
        assert(isinstance(size, Wad))
        assert((type >= 1) and (type <= 4))

        type_descs = {1: '开多', 2: '开空', 3: '平多', 4: '平空'}
        self.logger.info(f"Placing order ({type}-{type_descs[type]}, size {size} of {instrument_id},"
                         f" price {price})...")

        try:
            result = self._http_post("/api/swap/v3/order", {
                'instrument_id': instrument_id,
                'type': type,
                'price': str(price),
                'size': str(int(size))
            })
            order_id = str(result['order_id'])
            bol_result = bool(result['result'])

            self.logger.info(f"Placed order ({type}-{type_descs[type]}, size {size} of {instrument_id},"
                         f" price {price}) as #{order_id}, result {bol_result}")

            return order_id
        except:
            return -1

    def cancel_order(self, instrument_id: str, order_id: str) -> bool:
        """撤销之前下的未完成订单。
        POST /api/swap/v3/cancel_order/BTC-USD-SWAP/64-2b-17122f911-3
        """
        assert(isinstance(instrument_id, str))
        assert(isinstance(order_id, str))

        self.logger.info(f"Cancelling order #{order_id}...")

        result = self._http_post(f"/api/swap/v3/cancel_order/{instrument_id}/{order_id}")

        self.logger.info(f"Cancel order ({order_id} of {instrument_id}, result {result}")

        return result

    def get_candles(self, instrument_id: str, end: str, granularity: int = 60) -> list:
        """获取合约的K线数据。 获取截止时间的前200条
        GET /api/swap/v3/instruments/BTC-USD-SWAP/candles?start=2018-10-26T02:31:00.000Z&end=2018-10-26T02:55:00.000Z&granularity=60
        (查询BTC-USD-SWAP的2018年10月26日02点31分到2018年10月26日02点55分的1分钟K线数据)
        如果用户提供了开始时间或结束时间中的任一字段或全都提供，则取开始时间之后的200条数据或结束时间之前的200条数据或开始时间到结束时间的200条数据。
        未提供开始时间和结束时间的请求，则系统按时间粒度返回最近的200条数据。
        """

        self.logger.info(f"Get_candles {instrument_id} to {end} with {granularity}secs candle...")

        result = self._http_get(f"/api/swap/v3/instruments/{instrument_id}/candles",
                                f"end={end}&granularity={granularity}")
        return result

    @staticmethod
    def _filter_order(item: dict) -> bool:
        assert(isinstance(item, dict))
        return item['type'] in ['1', '2', '3', '4']

    @staticmethod
    def _parse_order(item: dict) -> Order:
        assert(isinstance(item, dict))

        return Order(order_id=str(item['order_id']),
                     timestamp=str(item['timestamp']),
                     instrument_id=str(item['instrument_id']),
                     type=int(item['type']),
                     price=Wad.from_number(item['price']),
                     size=Wad.from_number(item['size']),
                     filled_qty=Wad.from_number(item['filled_qty']),
                     fee=Wad.from_number(item['fee']),
                     status=int(item['status']),
                     contract_val=Wad.from_number(item['contract_val']))

    @staticmethod
    def _result(result, check_result: bool) -> dict:
        assert(isinstance(check_result, bool))

        if not result.ok:
            raise Exception(f"OKCoin API invalid HTTP response: {OKExSwapApi._http_response_summary(result)}")

        try:
            data = result.json()
        except Exception:
            raise Exception(f"OKCoin API invalid JSON response: {OKExSwapApi._http_response_summary(result)}")

        if check_result and 'error_code' in data:
            error_code = int(data['error_code'])
            if error_code > 0:
                raise Exception(f"OKCoin API negative response: {OKExSwapApi._http_response_summary(result)}")

        return data

    @staticmethod
    def _http_response_summary(response) -> str:
        text = response.text.replace('\r', '').replace('\n', '')[:2048]
        return f"{response.status_code} {response.reason} ({text})"

    def _get_timestamp(self):
        now = datetime.datetime.utcnow()
        t = now.isoformat()
        return t + "Z"

    def _get_server_timestamp(self):
        data = self._result(requests.get(f'{self.api_server}/api/general/v3/time'), False)
        return data['iso']

    def _okex_header(self, method, request_path, body=""):

        # timestamp = self._get_timestamp()
        timestamp = self._get_server_timestamp()

        """
        OK-ACCESS-SIGN的请求头是对timestamp + method + requestPath + body字符串(+表示字符串连接)，以及secretKey，使用HMAC SHA256方法加密，通过BASE64编码输出而得到的。
        """
        message = str(timestamp) + str.upper(method) + request_path + body
        mac = hmac.new(bytes(self.secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        d = mac.digest()
        sign = base64.b64encode(d)

        header = dict()
        header['Content-Type'] = "application/json"
        header['OK-ACCESS-KEY'] = self.api_key
        header['OK-ACCESS-SIGN'] = sign
        header['OK-ACCESS-TIMESTAMP'] = str(timestamp)
        header['OK-ACCESS-PASSPHRASE'] = self.passphrase
        return header

    def _http_get(self, resource: str, params: str = '', check_result: bool = True):
        assert(isinstance(resource, str))
        assert(isinstance(params, str))
        assert(isinstance(check_result, bool))

        if params != '':
            resource = f"{resource}?{params}"
        url = f"{self.api_server}{resource}"
        okex_header = self._okex_header('GET', resource)

        return self._result(requests.get(url=url,
                                         headers=okex_header,
                                         timeout=self.timeout), check_result)

    def _http_post(self, resource: str, params: dict):
        assert(isinstance(resource, str))
        assert(isinstance(params, dict))

        url = f"{self.api_server}{resource}"
        body = str(params)
        okex_header = self._okex_header('POST', resource, body)

        return self._result(requests.post(url=url,
                                          data=body,
                                          headers=okex_header,
                                          timeout=self.timeout), True)


class OKExSwapApiFactory:

    @staticmethod
    def get_okex_swap_api(config: dict):

        okexapi = OKExSwapApi(api_server=config["OKEX_API_SERVER"],
                              api_key=config["OKEX_API_KEY"],
                              secret_key=config["OKEX_API_SECRET"],
                              passphrase=config["OKEX_PASSPHRASE"],
                              timeout=10)

        return okexapi



