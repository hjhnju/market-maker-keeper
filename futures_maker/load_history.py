
from futures_maker.okex_websocket_api import HistoryDataFeed
import zlib
import datetime
from futures_maker.okex_api import OKExSwapApi

OKEX_API_SERVER = "https://www.okex.com"
OKEX_API_KEY = "e11624ea-20a1-4db4-9c8c-8d00a6dc4571"
OKEX_API_SECRET = "C0392340EE898C1F5861D946111E4856"
OKEX_PASSPHRASE = "Future20181230"


def inflate(data):
    decompress = zlib.decompressobj(
        -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


def load_history_ticker_data():
    okexapi = OKExSwapApi(api_server=OKEX_API_SERVER,
                          api_key=OKEX_API_KEY,
                          secret_key=OKEX_API_SECRET,
                          passphrase=OKEX_PASSPHRASE,
                          timeout=10)

    # OKEX接口限制只能取最多2000条
    set_end_time = datetime.datetime.now().isoformat() + "Z"
    candles = []
    with open('candles60_ETH-USD-SWAP.txt', 'wt') as f:
        for i in range(1, 11):
            print(f"read candle {i}, {set_end_time}")
            candles = okexapi.get_candles('ETH-USD-SWAP', set_end_time)
            for candle in candles:
                item = {"table": "swap/candle60s",
                        "data": {"instrument_id": 'ETH-USD-SWAP', "candle": candle}}
                f.write(f"{item}\n")
            f.flush()
            if len(candles) == 0:
                break
            set_end_time = candles[-1][0]

    print(len(candles))
    print(candles[-1])
    pass


if __name__ == '__main__':
    load_history_ticker_data()
    # 再按时间排序一下才能给回测用
    # cat candles60_ETH-USD-SWAP.txt | sort > candles60_ETH-USD-SWAP.txt1
