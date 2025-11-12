import pandas as pd

class ETFHoldings:
    def __init__(self, r):
        datalists = r['dataList']


        self.name = [i.get('name') for i in datalists]
        self.changeRatio = [round(float(i.get('changeRatio', 0))*100,2) for i in datalists]
        self.shareNumber = [i.get('shareNumber', 0) for i in datalists]
        self.ratio = [round(float(i.get('ratio', 0))*100,2) for i in datalists]


        self.tickerTuple = [i.get('tickerTuple', None) for i in datalists]
        self.tickerId = [i.get('tickerId', None) for i in self.tickerTuple]
        self.etfname = [i.get('name', None) for i in self.tickerTuple]
        self.symbol = [i.get('symbol', None) for i in self.tickerTuple]



        self.data_dict = { 
            'name': self.name,
            'change_ratio': self.changeRatio,
            'share_number': self.shareNumber,
            'ratio': self.ratio,
            'ticker_id': self.tickerId,
            'etf_name': self.etfname,
            'symbol': self.symbol
        }
        self.df = pd.DataFrame(self.data_dict)