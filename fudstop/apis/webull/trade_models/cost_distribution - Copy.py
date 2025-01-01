import pandas as pd
class CostDistribution:
    def __init__(self, r, ticker):
        all_data_dicts = []
        datas = r['data']
        self.all_profit_ratios = []
        for data in datas:
            self.tickerId = data['tickerId']
            self.avgCost = data['avgCost'] if 'avgCost' in data else None
            self.closeProfitRatio = data['closeProfitRatio'] if 'closeProfitRatio' in data else None
            self.chip90Start = data['chip90Start'] if 'chip90Start' in data else None
            self.chip90End = data['chip90End'] if 'chip90End' in data else None
            self.chip90Ratio = data['chip90Ratio'] if 'chip90Ratio' in data else None
            self.chip70Start = data['chip70Start'] if 'chip70Start' in data else None
            self.chip70End = data['chip70End'] if 'chip70end' in data else None
            self.chip70Ratio = data['chip70Ratio'] if 'chip70Ratio' in data else None
            self.close = data['close'] if 'close' in data else None
            self.totalShares = data['totalShares'] if 'totalShares' in data else None
            self.distributions = data['distributions'] if 'distributions' in data else None
            self.tradeStamp = data['tradeStamp'] if 'tradeStamp' in data else None


            self.data_dict = { 
                'ticker': ticker,
                'Avg Cost': self.avgCost,
                '% Shareholders in Profit': self.closeProfitRatio,
                'Close': self.close,
                'Total Shares': self.totalShares,
                'Distributions': self.distributions,
                
            }
            all_data_dicts.append(self.data_dict)
            self.all_profit_ratios.append(self.closeProfitRatio)
        try:
            self.df = pd.DataFrame(all_data_dicts, index=[0])
        except Exception as e:
            self.df = pd.DataFrame(all_data_dicts)


    def __str__(self):
        return f"CostDistribution(tickerId={self.tickerId}, avgCost={self.avgCost}, closeProfitRatio={self.closeProfitRatio}, chip90Start={self.chip90Start}, chip90End={self.chip90End}, chip90Ratio={self.chip90Ratio}, chip70Start={self.chip70Start}, chip70End={self.chip70End}, chip70Ratio={self.chip70Ratio}, close={self.close}, totalShares={self.totalShares}, distributions={self.distributions}, tradeStamp={self.tradeStamp})"

    def __repr__(self):
        return self.__str__()



class NewCostDist:
    def __init__(self, data, ticker):
        self.tickerId = [i.get('tickerId') for i in data]
        self.avgCost = [float(i.get('avgCost')) for i in data]
        self.closeProfitRatio = [round(float(i.get('closeProfitRatio'))*100,2) for i in data]
        self.chip90Start = [i.get('chip90Start') for i in data]
        self.chip90End = [i.get('chip90End') for i in data]
        self.chip90Ratio = [i.get('chip90Ratio') for i in data]
        self.chip70Start = [i.get('chip70Start') for i in data]
        self.chip70End = [i.get('chip70End') for i in data]
        self.chip70Ratio = [i.get('chip70Ratio') for i in data]
        self.close = [float(i.get('close')) for i in data]
        self.totalShares = [i.get('totalShares') for i in data]
        self.tradeStamp = [i.get('tradeStamp') for i in data]
        self.dates = [pd.to_datetime(ts, unit='ms').strftime('%Y-%m-%d') for ts in self.tradeStamp]


        self.data_dict = { 
            'ticker': [ticker] * len(self.close),
            'date': self.dates,
            'price': self.close,
            'profit_ratio': self.closeProfitRatio,
            'average_cost': self.avgCost
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)
