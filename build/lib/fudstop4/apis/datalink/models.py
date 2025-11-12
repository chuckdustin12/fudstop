import pandas as pd



class RetailActivity:
    def __init__(self, data):
        self.data = [item for sublist in data for item in sublist]
        self.results = data[0]
        self.date = self.results[0]
        self.ticker = self.results[1]
        self.activity = self.results[2]
        self.sentiment = self.results[3]




        self.data_dict = {  
            'date': self.date,
            'ticker': self.ticker,
            'activity': self.activity,
            'sentiment': self.sentiment
        }




        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])




class OptionsRank:
    def __init__(self, data):
        self.data = [item for sublist in data for item in sublist]
        
        self.results = data[0]

        self.ticker = self.results[0]

        self.date = self.results[1]
        self.crush_rate = self.results[2]
        self.td_until_er = self.results[4]
        self.cd_until_er = self.results[3]
        self.liquidity = self.results[5]
        self.has_leaps = self.results[6]
        self.has_weekly = self.results[7]
        self.iv30rank = self.results[8]
        self.iv30percentile = self.results[9]
        self.iv30rating = self.results[10]
        self.iv60rank = self.results[11]
        self.iv60percentile = self.results[12]
        self.iv60rating = self.results[13]
        self.iv90rank = self.results[14]
        self.iv90percentile = self.results[15]
        self.iv90rating = self.results[16]
        self.iv360rank = self.results[17]
        self.iv360percentile = self.results[18]
        self.iv360rating = self.results[19]


        self.data_dict = { 
            'date': self.date,
            'er_crush': self.crush_rate,
            'td_until_er': self.td_until_er,
            'cd_until_er': self.cd_until_er,
            'liquidity': self.liquidity,
            'leaps': self.has_leaps,
            'weekly': self.has_weekly,
            'iv30rank': self.iv30rank,
            'iv30percentile': self.iv30percentile,
            'iv30rating': self.iv30rating,
            'iv60rank': self.iv60rank,
            'iv60percentile': self.iv60percentile,
            'iv60rating': self.iv60rating,
            'iv90rank': self.iv90rank,
            'iv90percentile': self.iv90percentile,
            'iv90rating': self.iv90rating,
            'iv360rank': self.iv360rank,
            'iv360percentile': self.iv360percentile,
            'iv360rating': self.iv360rating
        }


        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])