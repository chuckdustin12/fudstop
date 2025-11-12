import pandas as pd



class MultiQuote:
    def __init__(self, data):
        self.tickerId = [float(i.get('tickerId', 0) or 0) for i in data]
        self.name = [i.get('name', '') for i in data]
        self.symbol = [i.get('symbol', '') for i in data]
        self.mkTradeTime = [i.get('mkTradeTime', 0) for i in data]
        self.close = [float(i.get('close', 0) or 0) for i in data]
        self.change = [float(i.get('change', 0) or 0) for i in data]
        self.changeRatio = [float(i.get('changeRatio', 0) or 0) for i in data]
        self.marketValue = [float(i.get('marketValue', 0) or 0) for i in data]
        self.volume = [float(i.get('volume', 0) or 0) for i in data]
        self.turnoverRate = [float(i.get('turnoverRate', 0) or 0) for i in data]
        self.overnight = [float(i.get('overnight', 0) or 0) for i in data]
        self.preClose = [float(i.get('preClose', 0) or 0) for i in data]
        self.open = [float(i.get('open', 0) or 0) for i in data]
        self.high = [float(i.get('high', 0) or 0) for i in data]
        self.low = [float(i.get('low', 0) or 0) for i in data]
        self.vibrateRatio = [float(i.get('vibrateRatio', 0) or 0) for i in data]
        self.avgVol10D = [float(i.get('avgVol10D', 0) or 0) for i in data]
        self.avgVol3M = [float(i.get('avgVol3M', 0) or 0) for i in data]
        self.negMarketValue = [float(i.get('negMarketValue', 0) or 0) for i in data]
        self.pe = [float(i.get('pe', 0) or 0) for i in data]
        self.forwardPe = [float(i.get('forwardPe', 0) or 0) for i in data]
        self.indicatedPe = [float(i.get('indicatedPe', 0) or 0) for i in data]
        self.peTtm = [float(i.get('peTtm', 0) or 0) for i in data]
        self.eps = [float(i.get('eps', 0) or 0) for i in data]
        self.epsTtm = [float(i.get('epsTtm', 0) or 0) for i in data]
        self.pb = [float(i.get('pb', 0) or 0) for i in data]
        self.totalShares = [float(i.get('totalShares', 0) or 0) for i in data]
        self.outstandingShares = [float(i.get('outstandingShares', 0) or 0) for i in data]        
        self.fiftyTwoWkHigh = [float(i.get('fiftyTwoWkHigh', 0) or 0) for i in data]
        self.fiftyTwoWkLow = [float(i.get('fiftyTwoWkLow', 0) or 0) for i in data]
        self.dividend = [float(i.get('dividend', 0) or 0) for i in data]
        self.yield_ = [float(i.get('yield', 0) or 0) for i in data]
        self.latestDividendDate = [i.get('latestDividendDate', '') for i in data]      
        self.latestSplitDate = [i.get('latestSplitDate', '') for i in data]
        self.latestEarningsDate = [i.get('latestEarningsDate', '') for i in data]      
        self.ps = [float(i.get('ps', 0) or 0) for i in data]
        self.bps = [float(i.get('bps', 0) or 0) for i in data]
        self.estimateEarningsDate = [i.get('estimateEarningsDate', '') for i in data]  
        self.nextEarningDay = [i.get('nextEarningDay', '') for i in data]
        self.data_dict = {
            'ticker_id': self.tickerId,
            'name': self.name,
            'symbol': self.symbol,
            'mk_trade_time': self.mkTradeTime,
            'close': self.close,
            'change': self.change,
            'change_ratio': self.changeRatio,
            'market_value': self.marketValue,
            'volume': self.volume,
            'turnover_rate': self.turnoverRate,
            'overnight': self.overnight,
            'pre_close': self.preClose,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'vibrate_ratio': self.vibrateRatio,
            'avg_vol_10d': self.avgVol10D,
            'avg_vol_3m': self.avgVol3M,
            'neg_market_value': self.negMarketValue,
            'pe': self.pe,
            'forward_pe': self.forwardPe,
            'indicated_pe': self.indicatedPe,
            'pe_ttm': self.peTtm,
            'eps': self.eps,
            'eps_ttm': self.epsTtm,
            'pb': self.pb,
            'total_shares': self.totalShares,
            'outstanding_shares': self.outstandingShares,
            'fifty_high': self.fiftyTwoWkHigh,
            'fifty_low': self.fiftyTwoWkLow,
            'dividend': self.dividend,
            'yield': self.yield_,
            'latest_dividend_date': self.latestDividendDate,
            'latest_split_date': self.latestSplitDate,
            'latest_earnings_date': self.latestEarningsDate,
            'ps': self.ps,
            'bps': self.bps,
            'estimate_earnings_date': self.estimateEarningsDate,
            'next_earning_day': self.nextEarningDay
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)