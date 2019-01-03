# 数据流操作类

import json
import logging
import threading
import time
from base64 import b64encode
import zlib
from urllib.parse import urlparse
import websocket
from market_maker_keeper.util import sanitize_url


class OKExWebsocketApi:
    logger = logging.getLogger()

    def __init__(self, ws_url: str, reconnect_delay: int = 5):
        super().__init__()
        assert (isinstance(ws_url, str))
        assert (isinstance(reconnect_delay, int))

        self.ws_url = ws_url
        self.reconnect_delay = reconnect_delay
        self._header = self._get_header(self.ws_url)
        self._sanitized_url = sanitize_url(self.ws_url)

        self._lock = threading.Lock()
        self._callback_function = None
        self.open_messages = []

    def lisen(self, open_messages: [str], callback):
        assert(callable(callback))

        self.open_messages = open_messages
        self._callback_function = callback
        threading.Thread(target=self._background_run, daemon=True).start()

    @staticmethod
    def _get_header(ws_url: str):
        parsed_url = urlparse(ws_url)

        if parsed_url.username is not None and parsed_url.password is not None:
            basic_header = b64encode(bytes(parsed_url.username + ":" + parsed_url.password, "utf-8")).decode("utf-8")
            return ["Authorization: Basic %s" % basic_header]
        return None

    def _background_run(self):
        while True:
            ws = websocket.WebSocketApp(url=self.ws_url,
                                        header=self._header,
                                        on_message=self._on_message,
                                        on_error=self._on_error,
                                        on_open=self._on_open,
                                        on_close=self._on_close)
            ws.run_forever(ping_interval=15, ping_timeout=10)
            time.sleep(self.reconnect_delay)

    def _on_open(self, ws):
        self.logger.info(f"WebSocket '{self._sanitized_url}' connected")
        for message in self.open_messages:
            ws.send(message)
            self.logger.info(f"Subscribe {message} sended")

    def _on_close(self, ws):
        self.logger.info(f"WebSocket '{self._sanitized_url}' disconnected")

    @staticmethod
    def inflate(data):
        decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
        )
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated

    def _on_message(self, ws, message):
        try:
            message = self.inflate(message).decode()
            message_dict = json.loads(message)
            if 'data' not in message_dict:
                self.logger.debug(f"ReceivedMsg '{message}', do nothing")
                return

            if self._callback_function is not None:
                self._callback_function(message_dict)

            self.logger.debug(f"WebSocket '{self._sanitized_url}' received message: '{message}'")
        except:
            self.logger.warning(f"WebSocket '{self._sanitized_url}' received invalid message: '{message}'")

    def _on_error(self, ws, error):
        self.logger.info(f"WebSocket '{self._sanitized_url}' error: '{error}'")



