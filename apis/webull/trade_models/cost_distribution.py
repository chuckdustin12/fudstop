import pandas as pd
class CostDistribution:
    def __init__(self, r):
        all_data_dicts = []
        datas = r['data']
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
                'Avg Cost': self.avgCost,
                '% Shareholders in Profit': self.closeProfitRatio,
                'Close': self.close,
                'Total Shares': self.totalShares,
                'Distributions': self.distributions,
                
            }
            all_data_dicts.append(self.data_dict)

        self.df = pd.DataFrame(all_data_dicts)


    def __str__(self):
        return f"CostDistribution(tickerId={self.tickerId}, avgCost={self.avgCost}, closeProfitRatio={self.closeProfitRatio}, chip90Start={self.chip90Start}, chip90End={self.chip90End}, chip90Ratio={self.chip90Ratio}, chip70Start={self.chip70Start}, chip70End={self.chip70End}, chip70Ratio={self.chip70Ratio}, close={self.close}, totalShares={self.totalShares}, distributions={self.distributions}, tradeStamp={self.tradeStamp})"

    def __repr__(self):
        return self.__str__()
