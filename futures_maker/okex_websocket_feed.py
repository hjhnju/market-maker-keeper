# 数据流操作类

import json
import logging
import threading
import time
import datetime
from base64 import b64encode
import zlib
from urllib.parse import urlparse
import websocket
from market_maker_keeper.util import sanitize_url


class OkexWebSocketFeed:
    logger = logging.getLogger()

    def __init__(self, ws_url: str, open_message: str = None, reconnect_delay: int = 5):
        super().__init__()
        assert (isinstance(ws_url, str))
        assert (isinstance(reconnect_delay, int))

        self.ws_url = ws_url
        self.reconnect_delay = reconnect_delay
        self._header = self._get_header(self.ws_url)
        self._sanitized_url = sanitize_url(self.ws_url)
        self._lock = threading.Lock()
        self._callback = None
        self.open_message = open_message

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
        if self.open_message:
            ws.send(self.open_message)
            self.logger.info(f"Subscribe {self.open_message} sended")

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

            # save data for analyse
            receive_time = datetime.datetime.now().isoformat() + 'Z'
            receive_time_utc = datetime.datetime.utcnow().isoformat() + 'Z'

            if callable(self._callback):
                self._callback(message_dict)

            self.logger.debug(f"[WebSocket Message]{receive_time}\t{receive_time_utc}\t{message_dict}\n")
        except:
            self.logger.warning(f"WebSocket '{self._sanitized_url}' received invalid message: '{message}'")

    def _on_error(self, ws, error):
        self.logger.info(f"WebSocket '{self._sanitized_url}' error: '{error}'")

    def set_callback(self, callback):
        assert(callable(callback))
        self._callback = callback



