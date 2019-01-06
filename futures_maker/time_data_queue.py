from datetime import datetime
from dateutil import parser
import pandas as pd


class TimeSeriesData:

    def __init__(self, instrument_id: str, number_limit: int):
        self.number_limit = number_limit
        self.instrument_id = instrument_id

        self.candle_df = pd.DataFrame(data=None,
                                           columns=['timestamp', 'open', 'high', 'low',
                                                    'close', 'volume', 'currency_volume', 'percent'])

    def push(self, item: dict, receive_time: str = None):
        """{'table': 'swap/candle60s', 'data': [
                                {'instrument_id': 'ETH-USD-SWAP',
                                'candle': ['2019-01-06T07:05:00.000Z', '150.37', '150.42', '150.33', '150.33', '926', '61.5865']
                                }
                            ]
            }"""
        if receive_time is None:
            receive_time = datetime.utcnow().isoformat() + 'Z'

        candle_pipes = {
            "swap/candle60s",
            "swap/candle300s",
            "swap/candle900s",
        }
        if item['table'] not in candle_pipes:
            return

        for row in item['data']:
            if row['instrument_id'] == self.instrument_id:
                candle = list(row['candle'])
                if len(candle) < 6:
                    continue

                candle_new = {}
                candle_new['timestamp'] = str(candle[0])
                candle_new['open'] = float(candle[1])
                candle_new['high'] = float(candle[2])
                candle_new['low'] = float(candle[3])
                candle_new['close'] = float(candle[4])
                candle_new['volume'] = float(candle[5])
                candle_new['currency_volume'] = float(candle[6])
                candle_new['percent'] = (candle_new['close'] - candle_new['open'])/candle_new['open']
                self.candle_df.loc[receive_time] = candle_new

        pass

    def __str__(self):
        return self.candle_df

    def max_percent_row(self):
        return self.candle_df.loc[self.candle_df['percent'].idxmax()]

    def filter_percent(self, lowest_percent: float):
        return self.candle_df[self.candle_df['percent'] > lowest_percent]


if __name__ == '__main__':
    import json

    kline = TimeSeriesData('ETH-USD-SWAP', 100)
    with open("swap.data", newline="\n") as f:
        line = f.readline()
        while line:
            row = line.split('\t')
            item = json.loads(row[1].replace('\'', '"'))
            kline.push(item, row[0])
            line = f.readline()

    print(kline.max_percent_row())

    print(kline.filter_percent(0.0043))



