import pandas as pd
import pytz
from datetime import datetime, timezone


class OrderFlow:
    def __init__(self, data):
        eastern = pytz.timezone('US/Eastern')
        self.totalNum = float(data.get('totalNum'))
        self.totalVolume = float(data.get('totalVolume'))
        self.avePrice = float(data.get('avePrice'))
        self.buyVolume = float(data.get('buyVolume'))
        self.sellVolume = float(data.get('sellVolume'))
        self.nVolume = float(data.get('nVolume'))
        self.preClose = float(data.get('preClose'))
        self.maxT = float(data.get('maxT'))
        

        stats = data.get('stats', [])


        self.price = [float(i.get('price')) for i in stats]
        self.type = [float(i.get('t')) for i in stats]
        self.buy_vol = [float(i.get('b')) for i in stats]
        self.sell_vol = [float(i.get('s')) for i in stats]
        self.neut_vol = [float(i.get('n')) for i in stats]
        self.ratio = [round(float(i.get('ratio'))*200,2) for i in stats]

        self.trade_time = data.get('tradeTimeTs')

        eastern = pytz.timezone('US/Eastern')
        unix_timestamp_s = self.trade_time / 1000
        

        eastern = pytz.timezone('US/Eastern')
        self.dt = datetime.fromtimestamp(unix_timestamp_s, tz=timezone.utc).astimezone(eastern)
        self.dt = self.dt.strftime('%Y-%m-%d %H:%M:%S')
        # self.base_data_dict = { 
        #     'total_number': self.totalNum,
        #     'total_vol': self.totalVolume,
        #     'avg_price': self.avePrice,
        #     'buy_vol': self.buyVolume,
        #     'sell_vol': self.sell_vol,
        #     'neut_vol': self.neut_vol,
        #     'pre_close': self.preClose,
        #     'maxT': self.maxT
        # }

        # self.base_df = pd.DataFrame(self.base_data_dict)


        self.order_data_dict = { 
            'time': self.dt,
            'price': self.price,
            'total_vol': self.type,
            'buy_vol': self.buy_vol,
            'sell_vol': self.sell_vol,
            'neut_vol': self.neut_vol,
            'ratio': self.ratio,

                            }
        

        self.order_df = pd.DataFrame(self.order_data_dict)