import pandas as pd
from fudstop.apis.helpers import to_datetime_eastern



class Deals:
    def __init__(self, data):

        self.tradeTime = [to_datetime_eastern(i.get('tradeTime')) for i in data]
        self.timestamp = [to_datetime_eastern(i.get('timestamp')) for i in data]
        self.tradeStamp = [to_datetime_eastern(i.get('tradeStamp')) for i in data]
        self.price = [float(i.get('price')) for i in data]
        self.volume = [float(i.get('volume')) for i in data]
        self.trdBs = [i.get('trdBs') for i in data]
        self.trdSeq = [i.get('trdSeq') for i in data]


        self.data_dict = { 
            'trade_time': self.tradeTime,
            'trade_stamp': self.tradeStamp,
            'timestamp': self.timestamp,
            'price': self.price,
            'volume': self.volume,
            'trd_bs': self.trdBs,
            'trd_seq': self.trdSeq
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)