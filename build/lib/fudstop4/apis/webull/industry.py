import pandas as pd



class IndustryData:
    def __init__(self, data):


        self.industry_name = [i.get('name') for i in data]
        self.industry_changeRatio = [round(float(i.get('changeRatio',0))*100,2) for i in data]
        self.industry_marketValue = [float(i.get('marketValue',0)) for i in data]
        self.industry_volume = [float(i.get('volume',0)) for i in data]
        self.declinedNum = [float(i.get('declinedNum',0)) for i in data]
        self.advancedNum = [float(i.get('advancedNum',0)) for i in data]
        self.flatNum = [float(i.get('flatNum',0)) for i in data]
        self.latestUpdateTime = [i.get('latestUpdateTime') for i in data]
        self.img = [i.get('img') for i in data]

        self.data = [i.get('data') for i in data]
        self.data = [item for sublist in self.data for item in sublist]

        ticker = [i.get('ticker') for i in self.data]

        self.tickerId = [i.get('tickerId') for i in ticker]
        self.exchangeId = [i.get('exchangeId') for i in ticker]
        self.name = [i.get('name') for i in ticker]
        self.symbol = [i.get('symbol') for i in ticker]
        self.tradeTime = [i.get('tradeTime') for i in ticker]
        self.close = [float(i.get('close',0)) for i in ticker]
        self.change = [float(i.get('change',0)) for i in ticker]
        self.changeRatio = [round(float(i.get('changeRatio',0))*100,2) for i in ticker]
        self.marketValue = [float(i.get('marketValue',0)) for i in ticker]
        self.volume = [float(i.get('volume',0)) for i in ticker]
        self.turnoverRate = [float(i.get('turnoverRate',0)) for i in ticker]
        self.fiftyTwoWkHigh = [float(i.get('fiftyTwoWkHigh',0)) for i in ticker]
        self.fiftyTwoWkLow = [float(i.get('fiftyTwoWkLow',0)) for i in ticker]
        self.open = [float(i.get('open',0)) for i in ticker]
        self.high = [float(i.get('high',0)) for i in ticker]
        self.low = [float(i.get('low',0)) for i in ticker]
        self.vibrateRatio = [float(i.get('vibrateRatio',0)) for i in ticker]
        self.amount = [float(i.get('amount',0)) for i in ticker]


        self.data_dict = { 
            'name': self.industry_name,
            'change_ratio': self.industry_changeRatio,
            'market_value': self.industry_marketValue,
            'volume': self.industry_volume,
            'declined_num': self.declinedNum,
            'advanced_num': self.advancedNum,
            'flat_num': self.flatNum,

        
            }
        

        
        # Creating ticker level DataFrame
        ticker_data_dict = {
            'ticker_id': self.tickerId,
            'exchange_id': self.exchangeId,
            'name': self.name,
            'symbol': self.symbol,
            'trade_time': self.tradeTime,
            'close': self.close,
            'change': self.change,
            'change_ratio': self.changeRatio,
            'market_value': self.marketValue,
            'volume': self.volume,
            'turnover_rate': self.turnoverRate,
            'fifty_two_wk_high': self.fiftyTwoWkHigh,
            'fifty_two_wk_low': self.fiftyTwoWkLow,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'vibrate_ratio': self.vibrateRatio,
            'amount': self.amount
        }
        self.as_dataframe = pd.DataFrame(self.data_dict)
        self.ticker_df = pd.DataFrame(ticker_data_dict)

        # If you want to merge industry data with ticker data, you need a key to join them.
        # Assuming 'name' is the common key between industry and ticker data
        self.merged_df = self.ticker_df.merge(self.as_dataframe, left_on='name', right_on='name', how='left')