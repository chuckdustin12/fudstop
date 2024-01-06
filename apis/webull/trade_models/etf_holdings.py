import pandas as pd

class ETFHoldings:
    def __init__(self, r):
        datalists = r['dataList']
        all_data_dicts = []
        for datalist in datalists:

            self.name = datalist.get('name',None)
            self.changeRatio = datalist.get('changeRatio', None)
            self.shareNumber = datalist.get('shareNumber', None)
            self.ratio = datalist.get('ratio', None)


            self.tickerTuple = datalist.get('tickerTuple', None)
            self.tickerId = datalist.get('tickerId', None)
            self.etfname = datalist.get('name', None)
            self.symbol = datalist.get('symbol', None)



            self.data_dict = { 
                'name': self.name,
                'change_ratio': self.changeRatio,
                'share_number': self.shareNumber,
                'ratio': self.ratio,
                'ticker_id': self.tickerId,
                'etf_name': self.etfname,
                'symbol': self.symbol
            }
            all_data_dicts.append(self.data_dict)

        self.df = pd.DataFrame(all_data_dicts)