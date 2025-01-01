import datetime
import pandas as pd
import numpy as np
import talib

class StockLoans:
    def __init__(self, data):
        self.businessDate = [i['businessDate'] if i['businessDate'] is not None else None for i in data]
        self.newMarketLoanCount = [i['newMarketLoanCount'] if i['newMarketLoanCount'] is not None else None for i in data]
        self.totalMarketLoanVal = [i['totalMarketLoanVal'] if i['totalMarketLoanVal'] is not None else None for i in data]
        self.newBilateralLoanCount = [i['newBilateralLoanCount'] if i['newBilateralLoanCount'] is not None else None for i in data]
        self.totalBilateralLoanVal = [i['totalBilateralLoanVal'] if i['totalBilateralLoanVal'] is not None else None for i in data]
        
        self.data_dict = {
            'businessDate': self.businessDate,
            'newMarketLoanCount': self.newMarketLoanCount,
            'totalMarketLoanVal': self.totalMarketLoanVal,
            'newBilateralLoanCount': self.newBilateralLoanCount,
            'totalBilateralLoanVal': self.totalBilateralLoanVal
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)

class VolumeTotals:
    def __init__(self, data):
        self.totalVolume = float(data['totalVolume'])
        self.optionsVolume = float(data['optionsVolume'])
        self.futuresVolume = float(data['futuresVolume'])
        self.fiftytwo_week_high = float(data['fiftytwo_week_high'])
        self.fiftytwo_week_low = float(data['fiftytwo_week_low'])
        self.monthlyDailyAverage = float(data['monthlyDailyAverage'])
        self.yearlyDailyAverage = float(data['yearlyDailyAverage'])
        weekly_volume = data['weekly_volume']
        trade_date = [i['trade_date'] if i['trade_date'] is not None else None for i in weekly_volume]
        self.trade_dates = [datetime.datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d") for timestamp in trade_date]
        self.trade_volume = [i['trade_volume'] if i['trade_volume'] is not None else None for i in weekly_volume]


        self.data_dict = {
            'totalVolume': self.totalVolume,
            'optionsVolume': self.optionsVolume,
            'futuresVolume': self.futuresVolume,
            'fiftytwo_week_high': self.fiftytwo_week_high,
            'fiftytwo_week_low': self.fiftytwo_week_low,
            'monthlyDailyAverage': self.monthlyDailyAverage,
            'yearlyDailyAverage': self.yearlyDailyAverage,
            'trade_dates': self.trade_dates,
            'trade_volume': self.trade_volume
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)
from datetime import date, timezone

def flatten_json(json_obj):
    flattened = json_obj.copy()
    for idx, week in enumerate(flattened.pop('weekly_volume')):
        for key, value in week.items():
            # Convert the timestamp to a date object
            if key == 'trade_date':
                value = date.fromtimestamp(value / 1000)
            
            # Add it to the main dictionary with a new key
            new_key = f'day_{idx + 1}_{key}'
            flattened[new_key] = value
    
    return flattened



class StockLoans:
    def __init__(self, data):
        self.businessDate = [i['businessDate'] if i['businessDate'] is not None else None for i in data]
        self.newMarketLoanCount = [i['newMarketLoanCount'] if i['newMarketLoanCount'] is not None else None for i in data]
        self.totalMarketLoanVal = [i['totalMarketLoanVal'] if i['totalMarketLoanVal'] is not None else None for i in data]
        self.newBilateralLoanCount = [i['newBilateralLoanCount'] if i['newBilateralLoanCount'] is not None else None for i in data]
        self.totalBilateralLoanVal = [i['totalBilateralLoanVal'] if i['totalBilateralLoanVal'] is not None else None for i in data]
        
        self.data_dict = {
            'businessDate': self.businessDate,
            'newMarketLoanCount': self.newMarketLoanCount,
            'totalMarketLoanVal': self.totalMarketLoanVal,
            'newBilateralLoanCount': self.newBilateralLoanCount,
            'totalBilateralLoanVal': self.totalBilateralLoanVal
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)

class VolumeTotals:
    def __init__(self, data):
        self.totalVolume = float(data['totalVolume'])
        self.optionsVolume = float(data['optionsVolume'])
        self.futuresVolume = float(data['futuresVolume'])
        self.fiftytwo_week_high = float(data['fiftytwo_week_high'])
        self.fiftytwo_week_low = float(data['fiftytwo_week_low'])
        self.monthlyDailyAverage = float(data['monthlyDailyAverage'])
        self.yearlyDailyAverage = float(data['yearlyDailyAverage'])
        weekly_volume = data['weekly_volume']
        trade_date = [i['trade_date'] if i['trade_date'] is not None else None for i in weekly_volume]
        self.trade_dates = [datetime.datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d") for timestamp in trade_date]
        self.trade_volume = [i['trade_volume'] if i['trade_volume'] is not None else None for i in weekly_volume]


        self.data_dict = {
            'totalVolume': self.totalVolume,
            'optionsVolume': self.optionsVolume,
            'futuresVolume': self.futuresVolume,
            'fiftytwo_week_high': self.fiftytwo_week_high,
            'fiftytwo_week_low': self.fiftytwo_week_low,
            'monthlyDailyAverage': self.monthlyDailyAverage,
            'yearlyDailyAverage': self.yearlyDailyAverage,
            'trade_dates': self.trade_dates,
            'trade_volume': self.trade_volume
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)
from datetime import date

def flatten_json(json_obj):
    flattened = json_obj.copy()
    for idx, week in enumerate(flattened.pop('weekly_volume')):
        for key, value in week.items():
            # Convert the timestamp to a date object
            if key == 'trade_date':
                value = date.fromtimestamp(value / 1000)
            
            # Add it to the main dictionary with a new key
            new_key = f'day_{idx + 1}_{key}'
            flattened[new_key] = value
    
    return flattened



class DailyMarketShare:
    def __init__(self, entity):
        total_volume = entity.get('total_volume')
        self.exchange = [i.get('exchange', None) for i in total_volume]
        self.calls = [i.get('calls', None) for i in total_volume]
        self.puts = [i.get('puts', None) for i in total_volume]
        self.ratio = [i.get('ratio', None) for i in total_volume]
        self.volume = [i.get('volume', None) for i in total_volume]
        self.market_share = [i.get('market_share',None) for i in total_volume]


        self.data_dict = { 

            'exchange': self.exchange,
            'calls': self.calls,
            'puts': self.puts,
            'ratio': self.ratio,
            'volume': self.volume,
            'market_share': self.market_share
        }



        self.df = pd.DataFrame(self.data_dict)




class OCCMostActive:
    def __init__(self, data):
        self.symbol = [i.get('symbol') for i in data]
        self.companyName = [i.get('companyName') for i in data]
        self.optVolCall = [i.get('optVolCall') for i in data]
        self.optVolPut = [i.get('optVolPut') for i in data]
        self.optVol = [i.get('optVol') for i in data]
        self.ivx30 = [i.get('ivx30') for i in data]
        self.ivx30ChgPercent = [i.get('ivx30ChgPercent') for i in data]
        self.stockType = [i.get('stockType') for i in data]
        self.optVolCallPercent = [i.get('optVolCallPercent') for i in data]
        self.optVolPutPercent = [i.get('optVolPutPercent') for i in data]

        self.data_dict = {
            'symbol': self.symbol,
            'company': self.companyName,
            'call_volume': self.optVolCall,
            'put_volume': self.optVolPut,
            'total_volume': self.optVol,
            'iv30': self.ivx30,
            'iv30_change_percent': self.ivx30ChgPercent,
            'stock_type': self.stockType,
            'call_vol_percent': self.optVolCallPercent,
            'put_vol_percent': self.optVolPutPercent
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)




class StockInfo:
    def __init__(self, data, ticker):
        # Existing attributes initialization
        self.dividendDate = data.get('dividendDate') if data.get('dividendDate') is not None else None
        self.dividendAmount = float(data.get('dividendAmount')) if data.get('dividendAmount') is not None else None
        self.dividendFrequency = data.get('dividendFrequency') if data.get('dividendFrequency') is not None else None
        self._yield = float(data.get('yield')) if data.get('yield') is not None else None
        self.bid = float(data.get('bid')) if data.get('bid') is not None else None
        self.ask = float(data.get('ask')) if data.get('ask') is not None else None
        self.mid = float(data.get('mid')) if data.get('mid') is not None else None
        self.bidSize = float(data.get('bidSize')) if data.get('bidSize') is not None else None
        self.askSize = float(data.get('askSize')) if data.get('askSize') is not None else None
        self.optVol = float(data.get('optVol')) if data.get('optVol') is not None else None
        self.stockVol = float(data.get('stockVol')) if data.get('stockVol') is not None else None
        self.ivx7 = float(data.get('ivx7')) if data.get('ivx7') is not None else None
        self.ivx14 = float(data.get('ivx14')) if data.get('ivx14') is not None else None
        self.ivx21 = float(data.get('ivx21')) if data.get('ivx21') is not None else None
        self.ivx30 = float(data.get('ivx30')) if data.get('ivx30') is not None else None
        self.ivx60 = float(data.get('ivx60')) if data.get('ivx60') is not None else None
        self.ivx90 = float(data.get('ivx90')) if data.get('ivx90') is not None else None
        self.ivx120 = float(data.get('ivx120')) if data.get('ivx120') is not None else None
        self.ivx150 = float(data.get('ivx150')) if data.get('ivx150') is not None else None
        self.ivx180 = float(data.get('ivx180')) if data.get('ivx180') is not None else None
        self.ivx270 = float(data.get('ivx270')) if data.get('ivx270') is not None else None
        self.ivx360 = float(data.get('ivx360')) if data.get('ivx360') is not None else None
        self.ivx720 = float(data.get('ivx720')) if data.get('ivx720') is not None else None
        self.ivx1080 = float(data.get('ivx1080')) if data.get('ivx1080') is not None else None
        self.ivx7Chg = float(data.get('ivx7Chg')) if data.get('ivx7Chg') is not None else None
        self.ivx14Chg = float(data.get('ivx14Chg')) if data.get('ivx14Chg') is not None else None
        self.ivx21Chg = float(data.get('ivx21Chg')) if data.get('ivx21Chg') is not None else None
        self.ivx30Chg = float(data.get('ivx30Chg')) if data.get('ivx30Chg') is not None else None
        self.ivx60Chg = float(data.get('ivx60Chg')) if data.get('ivx60Chg') is not None else None
        self.ivx90Chg = float(data.get('ivx90Chg')) if data.get('ivx90Chg') is not None else None
        self.ivx120Chg = float(data.get('ivx120Chg')) if data.get('ivx120Chg') is not None else None
        self.ivx150Chg = float(data.get('ivx150Chg')) if data.get('ivx150Chg') is not None else None
        self.ivx180Chg = float(data.get('ivx180Chg')) if data.get('ivx180Chg') is not None else None
        self.ivx270Chg = float(data.get('ivx270Chg')) if data.get('ivx270Chg') is not None else None
        self.ivx360Chg = float(data.get('ivx360Chg')) if data.get('ivx360Chg') is not None else None
        self.ivx720Chg = float(data.get('ivx720Chg')) if data.get('ivx720Chg') is not None else None
        self.ivx1080Chg = float(data.get('ivx1080Chg')) if data.get('ivx1080Chg') is not None else None
        self.ivx7ChgPercent = float(data.get('ivx7ChgPercent')) if data.get('ivx7ChgPercent') is not None else None
        self.ivx14ChgPercent = float(data.get('ivx14ChgPercent')) if data.get('ivx14ChgPercent') is not None else None
        self.ivx21ChgPercent = float(data.get('ivx21ChgPercent')) if data.get('ivx21ChgPercent') is not None else None
        self.ivx30ChgPercent = float(data.get('ivx30ChgPercent')) if data.get('ivx30ChgPercent') is not None else None
        self.ivx60ChgPercent = float(data.get('ivx60ChgPercent')) if data.get('ivx60ChgPercent') is not None else None
        self.ivx90ChgPercent = float(data.get('ivx90ChgPercent')) if data.get('ivx90ChgPercent') is not None else None
        self.ivx120ChgPercent = float(data.get('ivx120ChgPercent')) if data.get('ivx120ChgPercent') is not None else None
        self.ivx150ChgPercent = float(data.get('ivx150ChgPercent')) if data.get('ivx150ChgPercent') is not None else None
        self.ivx180ChgPercent = float(data.get('ivx180ChgPercent')) if data.get('ivx180ChgPercent') is not None else None
        self.ivx270ChgPercent = float(data.get('ivx270ChgPercent')) if data.get('ivx270ChgPercent') is not None else None
        self.ivx360ChgPercent = float(data.get('ivx360ChgPercent')) if data.get('ivx360ChgPercent') is not None else None
        self.ivx720ChgPercent = float(data.get('ivx720ChgPercent')) if data.get('ivx720ChgPercent') is not None else None
        self.ivx1080ChgPercent = float(data.get('ivx1080ChgPercent')) if data.get('ivx1080ChgPercent') is not None else None
        self.ivx7ChgOpen = float(data.get('ivx7ChgOpen')) if data.get('ivx7ChgOpen') is not None else None
        self.ivx14ChgOpen = float(data.get('ivx14ChgOpen')) if data.get('ivx14ChgOpen') is not None else None
        self.ivx21ChgOpen = float(data.get('ivx21ChgOpen')) if data.get('ivx21ChgOpen') is not None else None
        self.ivx30ChgOpen = float(data.get('ivx30ChgOpen')) if data.get('ivx30ChgOpen') is not None else None
        self.ivx60ChgOpen = float(data.get('ivx60ChgOpen')) if data.get('ivx60ChgOpen') is not None else None
        self.ivx90ChgOpen = float(data.get('ivx90ChgOpen')) if data.get('ivx90ChgOpen') is not None else None
        self.ivx120ChgOpen = float(data.get('ivx120ChgOpen')) if data.get('ivx120ChgOpen') is not None else None
        self.ivx150ChgOpen = float(data.get('ivx150ChgOpen')) if data.get('ivx150ChgOpen') is not None else None
        self.ivx180ChgOpen = float(data.get('ivx180ChgOpen')) if data.get('ivx180ChgOpen') is not None else None
        self.ivx270ChgOpen = float(data.get('ivx270ChgOpen')) if data.get('ivx270ChgOpen') is not None else None
        self.ivx360ChgOpen = float(data.get('ivx360ChgOpen')) if data.get('ivx360ChgOpen') is not None else None
        self.ivx720ChgOpen = float(data.get('ivx720ChgOpen')) if data.get('ivx720ChgOpen') is not None else None
        self.ivx1080ChgOpen = float(data.get('ivx1080ChgOpen')) if data.get('ivx1080ChgOpen') is not None else None
        self.ivx7ChgPercentOpen = float(data.get('ivx7ChgPercentOpen')) if data.get('ivx7ChgPercentOpen') is not None else None
        self.ivx14ChgPercentOpen = float(data.get('ivx14ChgPercentOpen')) if data.get('ivx14ChgPercentOpen') is not None else None
        self.ivx21ChgPercentOpen = float(data.get('ivx21ChgPercentOpen')) if data.get('ivx21ChgPercentOpen') is not None else None
        self.ivx30ChgPercentOpen = float(data.get('ivx30ChgPercentOpen')) if data.get('ivx30ChgPercentOpen') is not None else None
        self.ivx60ChgPercentOpen = float(data.get('ivx60ChgPercentOpen')) if data.get('ivx60ChgPercentOpen') is not None else None
        self.ivx90ChgPercentOpen = float(data.get('ivx90ChgPercentOpen')) if data.get('ivx90ChgPercentOpen') is not None else None
        self.ivx120ChgPercentOpen = float(data.get('ivx120ChgPercentOpen')) if data.get('ivx120ChgPercentOpen') is not None else None
        self.ivx150ChgPercentOpen = float(data.get('ivx150ChgPercentOpen')) if data.get('ivx150ChgPercentOpen') is not None else None
        self.ivx180ChgPercentOpen = float(data.get('ivx180ChgPercentOpen')) if data.get('ivx180ChgPercentOpen') is not None else None
        self.ivx270ChgPercentOpen = float(data.get('ivx270ChgPercentOpen')) if data.get('ivx270ChgPercentOpen') is not None else None
        self.ivx360ChgPercentOpen = float(data.get('ivx360ChgPercentOpen')) if data.get('ivx360ChgPercentOpen') is not None else None
        self.ivx720ChgPercentOpen = float(data.get('ivx720ChgPercentOpen')) if data.get('ivx720ChgPercentOpen') is not None else None
        self.ivx1080ChgPercentOpen = float(data.get('ivx1080ChgPercentOpen')) if data.get('ivx1080ChgPercentOpen') is not None else None
        self.high = float(data.get('high')) if data.get('high') is not None else None
        self.low = float(data.get('low')) if data.get('low') is not None else None
        self.open = float(data.get('open')) if data.get('open') is not None else None
        self.price = float(data.get('price')) if data.get('price') is not None else None
        self.prevClose = float(data.get('prevClose')) if data.get('prevClose') is not None else None
        self.openInterest = float(data.get('openInterest')) if data.get('openInterest') is not None else None
        self.highPrice52Wk = float(data.get('highPrice52Wk')) if data.get('highPrice52Wk') is not None else None
        self.lowPrice52Wk = float(data.get('lowPrice52Wk')) if data.get('lowPrice52Wk') is not None else None
        self.change = float(data.get('change')) if data.get('change') is not None else None
        self.changePercent = float(data.get('changePercent')) if data.get('changePercent') is not None else None
        self.changeOpen = float(data.get('changeOpen')) if data.get('changeOpen') is not None else None
        self.changePercentOpen = float(data.get('changePercentOpen')) if data.get('changePercentOpen') is not None else None
        self.callVol = float(data.get('callVol')) if data.get('callVol') is not None else None
        self.putVol = float(data.get('putVol')) if data.get('putVol') is not None else None
        self.hv10 = float(data.get('hv10')) if data.get('hv10') is not None else None
        self.hv20 = float(data.get('hv20')) if data.get('hv20') is not None else None
        self.hv30 = float(data.get('hv30')) if data.get('hv30') is not None else None
        self.hv60 = float(data.get('hv60')) if data.get('hv60') is not None else None
        self.hv90 = float(data.get('hv90')) if data.get('hv90') is not None else None
        self.hv120 = float(data.get('hv120')) if data.get('hv120') is not None else None
        self.hv150 = float(data.get('hv150')) if data.get('hv150') is not None else None
        self.hv180 = float(data.get('hv180')) if data.get('hv180') is not None else None
        self.hvp10 = float(data.get('hvp10')) if data.get('hvp10') is not None else None
        self.hvp20 = float(data.get('hvp20')) if data.get('hvp20') is not None else None
        self.hvp30 = float(data.get('hvp30')) if data.get('hvp30') is not None else None
        self.hvp60 = float(data.get('hvp60')) if data.get('hvp60') is not None else None
        self.hvp90 = float(data.get('hvp90')) if data.get('hvp90') is not None else None
        self.hvp120 = float(data.get('hvp120')) if data.get('hvp120') is not None else None
        self.hvp150 = float(data.get('hvp150')) if data.get('hvp150') is not None else None
        self.hvp180 = float(data.get('hvp180')) if data.get('hvp180') is not None else None
        self.beta10D = float(data.get('beta10D')) if data.get('beta10D') is not None else None
        self.beta20D = float(data.get('beta20D')) if data.get('beta20D') is not None else None
        self.beta30D = float(data.get('beta30D')) if data.get('beta30D') is not None else None
        self.beta60D = float(data.get('beta60D')) if data.get('beta60D') is not None else None
        self.beta90D = float(data.get('beta90D')) if data.get('beta90D') is not None else None
        self.beta120D = float(data.get('beta120D')) if data.get('beta120D') is not None else None
        self.beta150D = float(data.get('beta150D')) if data.get('beta150D') is not None else None
        self.beta180D = float(data.get('beta180D')) if data.get('beta180D') is not None else None
        self.corr10D = float(data.get('corr10D')) if data.get('corr10D') is not None else None
        self.corr20D = float(data.get('corr20D')) if data.get('corr20D') is not None else None
        self.corr30D = float(data.get('corr30D')) if data.get('corr30D') is not None else None
        self.corr60D = float(data.get('corr60D')) if data.get('corr60D') is not None else None
        self.corr90D = float(data.get('corr90D')) if data.get('corr90D') is not None else None
        self.corr120D = float(data.get('corr120D')) if data.get('corr120D') is not None else None
        self.corr150D = float(data.get('corr150D')) if data.get('corr150D') is not None else None
        self.corr180D = float(data.get('corr180D')) if data.get('corr180D') is not None else None
        self.outstandingShares = float(data.get('outstandingShares')) if data.get('outstandingShares') is not None else None
        self.marketCap = float(data.get('marketCap')) if data.get('marketCap') is not None else None
        self.updateDate = data.get('updateDate') if data.get('updateDate') is not None else None
        self.atClose = data.get('atClose') if data.get('atClose') is not None else None
        self.currency = data.get('currency') if data.get('currency') is not None else None
        self.lastDate = data.get('lastDate') if data.get('lastDate') is not None else None
        self.eps = data.get('eps') if data.get('eps') is not None else None
        self.pe = data.get('pe') if data.get('pe') is not None else None
        self.industry = data.get('industry') if data.get('industry') is not None else None
        self.ivp30 = data.get('ivp30') if data.get('ivp30') is not None else None
        self.ivp60 = data.get('ivp60') if data.get('ivp60') is not None else None
        self.ivp90 = data.get('ivp90') if data.get('ivp90') is not None else None
        self.sentiment = data.get('sentiment') if data.get('sentiment') is not None else None
        self.volatileRank = data.get('volatileRank') if data.get('volatileRank') is not None else None
        self.ivr30 = data.get('ivr30') if data.get('ivr30') is not None else None
        self.ivr60 = data.get('ivr60') if data.get('ivr60') is not None else None
        self.ivr90 = data.get('ivr90') if data.get('ivr90') is not None else None
        self.ivr120 = data.get('ivr120') if data.get('ivr120') is not None else None
        self.ivr150 = data.get('ivr150') if data.get('ivr150') is not None else None
        self.ivr180 = data.get('ivr180') if data.get('ivr180') is not None else None
        self.avgOptVol1MO = data.get('avgOptVol1MO') if data.get('avgOptVol1MO') is not None else None
        self.avgOptOI1MO = data.get('avgOptOI1MO') if data.get('avgOptOI1MO') is not None else None

        # New metrics
        self.spread = self.ask - self.bid if self.ask is not None and self.bid is not None else None
        self.bidAskRatio = (self.bidSize / self.askSize) if self.bidSize is not None and self.askSize is not None and self.askSize != 0 else None
        self.dividendYield = (self.dividendAmount / self.price) * 100 if self.dividendAmount is not None and self.price is not None else None
        self.ivRank = ((self.ivx30 - self.ivx180) / self.ivx180) * 100 if self.ivx30 is not None and self.ivx180 is not None and self.ivx180 != 0 else None
        self.betaRatio = self.beta90D / self.beta30D if self.beta90D is not None and self.beta30D is not None and self.beta30D != 0 else None


      


        self.data_dict = {
            'ticker': ticker,
            'dividend_date': self.dividendDate,
            'dividend_amount': self.dividendAmount,
            'dividend_frequency': self.dividendFrequency,
            'yield': self._yield,
            'bid': self.bid,
            'ask': self.ask,
            'bid_ask_ratio': self.bidAskRatio,
            'mid': self.mid,
            'bid_size': self.bidSize,
            'ask_size': self.askSize,
            'opt_vol': self.optVol,
            'stock_vol': self.stockVol,
            'ivx7': self.ivx7,
            'ivx14': self.ivx14,
            'ivx21': self.ivx21,
            'ivx30': self.ivx30,
            'ivx60': self.ivx60,
            'ivx90': self.ivx90,
            'ivx120': self.ivx120,
            'ivx150': self.ivx150,
            'ivx180': self.ivx180,
            'ivx270': self.ivx270,
            'ivx360': self.ivx360,
            'ivx720': self.ivx720,
            'ivx1080': self.ivx1080,
            'ivx7_chg': self.ivx7Chg,
            'ivx14_chg': self.ivx14Chg,
            'ivx21_chg': self.ivx21Chg,
            'ivx30_chg': self.ivx30Chg,
            'ivx60_chg': self.ivx60Chg,
            'ivx90_chg': self.ivx90Chg,
            'ivx120_chg': self.ivx120Chg,
            'ivx150_chg': self.ivx150Chg,
            'ivx180_chg': self.ivx180Chg,
            'ivx270_chg': self.ivx270Chg,
            'ivx360_chg': self.ivx360Chg,
            'ivx720_chg': self.ivx720Chg,
            'ivx1080_chg': self.ivx1080Chg,
            'ivx7_chg_percent': self.ivx7ChgPercent,
            'ivx14_chg_percent': self.ivx14ChgPercent,
            'ivx21_chg_percent': self.ivx21ChgPercent,
            'ivx30_chg_percent': self.ivx30ChgPercent,
            'ivx60_chg_percent': self.ivx60ChgPercent,
            'ivx90_chg_percent': self.ivx90ChgPercent,
            'ivx120_chg_percent': self.ivx120ChgPercent,
            'ivx150_chg_percent': self.ivx150ChgPercent,
            'ivx180_chg_percent': self.ivx180ChgPercent,
            'ivx270_chg_percent': self.ivx270ChgPercent,
            'ivx360_chg_percent': self.ivx360ChgPercent,
            'ivx720_chg_percent': self.ivx720ChgPercent,
            'ivx1080_chg_percent': self.ivx1080ChgPercent,
            'ivx7_chg_open': self.ivx7ChgOpen,
            'ivx14_chg_open': self.ivx14ChgOpen,
            'ivx21_chg_open': self.ivx21ChgOpen,
            'ivx30_chg_open': self.ivx30ChgOpen,
            'ivx60_chg_open': self.ivx60ChgOpen,
            'ivx90_chg_open': self.ivx90ChgOpen,
            'ivx120_chg_open': self.ivx120ChgOpen,
            'ivx150_chg_open': self.ivx150ChgOpen,
            'ivx180_chg_open': self.ivx180ChgOpen,
            'ivx270_chg_open': self.ivx270ChgOpen,
            'ivx360_chg_open': self.ivx360ChgOpen,
            'ivx720_chg_open': self.ivx720ChgOpen,
            'ivx1080_chg_open': self.ivx1080ChgOpen,
            'ivx7_chg_percent_open': self.ivx7ChgPercentOpen,
            'ivx14_chg_percent_open': self.ivx14ChgPercentOpen,
            'ivx21_chg_percent_open': self.ivx21ChgPercentOpen,
            'ivx30_chg_percent_open': self.ivx30ChgPercentOpen,
            'ivx60_chg_percent_open': self.ivx60ChgPercentOpen,
            'ivx90_chg_percent_open': self.ivx90ChgPercentOpen,
            'ivx120_chg_percent_open': self.ivx120ChgPercentOpen,
            'ivx150_chg_percent_open': self.ivx150ChgPercentOpen,
            'ivx180_chg_percent_open': self.ivx180ChgPercentOpen,
            'ivx270_chg_percent_open': self.ivx270ChgPercentOpen,
            'ivx360_chg_percent_open': self.ivx360ChgPercentOpen,
            'ivx720_chg_percent_open': self.ivx720ChgPercentOpen,
            'ivx1080_chg_percent_open': self.ivx1080ChgPercentOpen,
            'high': self.high,
            'low': self.low,
            'open': self.open,
            'price': self.price,
            'prev_close': self.prevClose,
            'open_interest': self.openInterest,
            'high_price_52wk': self.highPrice52Wk,
            'low_price_52wk': self.lowPrice52Wk,
            'change': self.change,
            'change_percent': self.changePercent,
            'change_open': self.changeOpen,
            'change_percent_open': self.changePercentOpen,
            'call_vol': self.callVol,
            'put_vol': self.putVol,
            'hv10': self.hv10,
            'hv20': self.hv20,
            'hv30': self.hv30,
            'hv60': self.hv60,
            'hv90': self.hv90,
            'hv120': self.hv120,
            'hv150': self.hv150,
            'hv180': self.hv180,
            'hvp10': self.hvp10,
            'hvp20': self.hvp20,
            'hvp30': self.hvp30,
            'hvp60': self.hvp60,
            'hvp90': self.hvp90,
            'hvp120': self.hvp120,
            'hvp150': self.hvp150,
            'hvp180': self.hvp180,
            'beta10d': self.beta10D,
            'beta20d': self.beta20D,
            'beta30d': self.beta30D,
            'beta60d': self.beta60D,
            'beta90d': self.beta90D,
            'beta120d': self.beta120D,
            'beta150d': self.beta150D,
            'beta180d': self.beta180D,
            'beta_ratio': self.betaRatio,
            'corr10d': self.corr10D,
            'corr20d': self.corr20D,
            'corr30d': self.corr30D,
            'corr60d': self.corr60D,
            'corr90d': self.corr90D,
            'corr120d': self.corr120D,
            'corr150d': self.corr150D,
            'corr180d': self.corr180D,
            'outstanding_shares': self.outstandingShares,
            'market_cap': self.marketCap,
            'update_date': self.updateDate,
            'at_close': self.atClose,
            'currency': self.currency,
            'last_date': self.lastDate,
            'eps': self.eps,
            'pe': self.pe,
            'industry': self.industry,
            'ivp30': self.ivp30,
            'ivp60': self.ivp60,
            'ivp90': self.ivp90,
            'sentiment': self.sentiment,
            'volatile_rank': self.volatileRank,
            "volatility_per_share": self.stockVol / self.outstandingShares if self.stockVol is not None and self.outstandingShares is not None and self.outstandingShares != 0 else None,
            'ivr30': self.ivr30,
            'ivr60': self.ivr60,
            'ivr90': self.ivr90,
            'ivr120': self.ivr120,
            'ivr150': self.ivr150,
            'ivr180': self.ivr180,
            'iv_rank': self.ivRank,
            'avg_opt_vol_1mo': self.avgOptVol1MO,
            'avg_opt_oi_1mo': self.avgOptOI1MO,
            "iv_ratio": self.ivx30 / self.ivx180 if self.ivx30 is not None and self.ivx180 is not None and self.ivx180 != 0 else None,
            "earnings_yield": self.eps / self.price * 100 if self.eps is not None and self.price is not None and self.price != 0 else None,
            "dividend_payout_ratio": self.dividendAmount / self.eps * 100 if self.dividendAmount is not None and self.eps is not None and self.eps != 0 else None,
            "spread": self.spread,            
        }



        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])



class ExpiryDates:
    def __init__(self, data):
        
        self.expDate = [i.get('expDate') for i in data]
        self.daysToExp = [i.get('daysToExp') for i in data]
        self.daysToExp = [datetime.datetime.strftime(i.get('daysToExp'), tz=timezone.utc).replace(tzinfo=None) for i in data]
        self.hours = [i.get('hours') for i in data]
        self.minutes = [i.get('minutes') for i in data]



        self.data_dict = { 
            'expiry': self.expDate,
            'dte': self.daysToExp,
            'hours': self.hours,
            'minutes': self.minutes
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)




class OptionsMonitor:
    def __init__(self, data):

        self.call_change_eod = [float(i.get('call_change_eod', 0.0)) for i in data]
        self.call_ivbid = [float(i.get('call_ivbid', 0.0)) for i in data]
        self.call_iv_eod = [float(i.get('call_iv_eod', 0.0)) for i in data]
        self.put_theta_eod = [float(i.get('put_theta_eod', 0.0)) for i in data]
        self.call_ivask = [float(i.get('call_ivask', 0.0)) for i in data]
        self.call_days = [float(i.get('call_days', 0.0)) for i in data]
        self.call_mean_eod = [float(i.get('call_mean_eod', 0.0)) for i in data]
        self.put_asksize = [i.get('put_asksize', 0.0) for i in data]
        self.call_ivint = [float(i.get('call_ivint', 0.0)) for i in data]
        self.call_delta_eod = [float(i.get('call_delta_eod', 0.0)) for i in data]
        self.call_bid_eod = [float(i.get('call_bid_eod', 0.0)) for i in data]
        self.call_theoprice_eod = [float(i.get('call_theoprice_eod', 0.0)) for i in data]
        self.put_iv = [float(i.get('put_iv', 0.0)) for i in data]
        self.call_ivint_eod = [float(i.get('call_ivint_eod', 0.0)) for i in data]
        self.call_ask_eod = [float(i.get('call_ask_eod', 0.0)) for i in data]
        self.call_iv = [float(i.get('call_iv', 0.0)) for i in data]
        self.put_days = [float(i.get('put_days', 0.0)) for i in data]
        self.put_iv_eod = [float(i.get('put_iv_eod', 0.0)) for i in data]
        self.put_change_eod = [float(i.get('put_change_eod', 0.0)) for i in data]
        self.call_volume_eod = [float(i.get('call_volume_eod', 0.0)) for i in data]
        self.call_ask = [float(i.get('call_ask', 0.0)) for i in data]
        self.call_bidtime = [i.get('call_bidtime', 0.0) for i in data]
        self.call_rho = [float(i.get('call_rho', 0.0)) for i in data]
        self.call_forwardprice_eod = [float(i.get('call_forwardprice_eod', 0.0)) for i in data]
        self.call_mean = [float(i.get('call_mean', 0.0)) for i in data]
        self.put_bid_eod = [float(i.get('put_bid_eod', 0.0)) for i in data]
        self.call_bid = [float(i.get('call_bid', 0.0)) for i in data]
        self.call_volume = [float(i.get('call_volume', 0.0)) for i in data]
        self.call_alpha = [float(i.get('call_alpha', 0.0)) for i in data]
        self.put_bidtime = [i.get('put_bidtime', 0.0) for i in data]
        self.call_vega = [float(i.get('call_vega', 0.0)) for i in data]
        self.put_theta = [float(i.get('put_theta', 0.0)) for i in data]
        self.put_symbol = ["O:" + i.get('put_optionsymbol', '').replace(' ', '') for i in data]

        self.put_ivask = [float(i.get('put_ivask', 0.0)) for i in data]
        self.put_changepercent_eod = [float(i.get('put_changepercent_eod', 0.0)) for i in data]
        self.put_ask = [float(i.get('put_ask', 0.0)) for i in data]
        self.put_rho = [float(i.get('put_rho', 0.0)) for i in data]
        self.call_openinterest_eod = [float(i.get('call_openinterest_eod', 0.0)) for i in data]
        self.put_ivint = [float(i.get('put_ivint', 0.0)) for i in data]
        self.put_theoprice = [float(i.get('put_theoprice', 0.0)) for i in data]
        self.put_bid = [float(i.get('put_bid', 0.0)) for i in data]
        self.call_asktime = [i.get('call_asktime', 0.0) for i in data]
        self.put_ask_eod = [float(i.get('put_ask_eod', 0.0)) for i in data]
        self.call_gamma_eod = [float(i.get('call_gamma_eod', 0.0)) for i in data]
        self.call_symbol = ["O:" + i.get('call_optionsymbol', '').replace(' ', '') for i in data]
        self.put_paramvolapercent_eod = [float(i.get('put_paramvolapercent_eod', 0.0)) for i in data]
        self.put_volume = [float(i.get('put_volume', 0.0)) for i in data]
        self.call_asksize = [float(i.get('call_asksize', 0.0)) for i in data]
        self.call_alpha_eod = [float(i.get('call_alpha_eod', 0.0)) for i in data]
        self.put_volume_eod = [float(i.get('put_volume_eod', 0.0)) for i in data]
        self.put_ivbid = [float(i.get('put_ivbid', 0.0)) for i in data]
        self.call_pos = [float(i.get('call_pos', 0.0)) for i in data]
        self.put_delta_eod = [float(i.get('put_delta_eod', 0.0)) for i in data]
        self.put_changepercent = [float(i.get('put_changepercent', 0.0)) for i in data]
        self.put_mean_eod = [float(i.get('put_mean_eod', 0.0)) for i in data]
        self.call_changepercent = [float(i.get('call_changepercent', 0.0)) for i in data]
        self.put_asktime = [i.get('put_asktime', 0.0) for i in data]
        self.put_pos = [float(i.get('put_pos', 0.0)) for i in data]
        self.put_theoprice_eod = [float(i.get('put_theoprice_eod', 0.0)) for i in data]
        self.put_gamma = [float(i.get('put_gamma', 0.0)) for i in data]
        self.call_days_eod = [float(i.get('call_days_eod', 0.0)) for i in data]
        self.call_bidsize = [float(i.get('call_bidsize', 0.0)) for i in data]
        self.call_delta = [float(i.get('call_delta', 0.0)) for i in data]
        self.put_change = [float(i.get('put_change', 0.0)) for i in data]
        self.call_paramvolapercent_eod = [float(i.get('call_paramvolapercent_eod', 0.0)) for i in data]
        self.call_theta_eod = [float(i.get('call_theta_eod', 0.0)) for i in data]
        self.call_change = [float(i.get('call_change', 0.0)) for i in data]
        self.put_ivint_eod = [float(i.get('put_ivint_eod', 0.0)) for i in data]
        self.put_vega = [float(i.get('put_vega', 0.0)) for i in data]
        self.call_theta = [float(i.get('call_theta', 0.0)) for i in data]
        self.put_days_eod = [float(i.get('put_days_eod', 0.0)) for i in data]
        self.put_forwardprice = [float(i.get('put_forwardprice', 0.0)) for i in data]
        self.call_rho_eod = [float(i.get('call_rho_eod', 0.0)) for i in data]
        self.quotetime = [float(i.get('quotetime', 0.0)) for i in data]
        self.put_vega_eod = [float(i.get('put_vega_eod', 0.0)) for i in data]
        self.strike = [float(i.get('strike', 0.0)) for i in data]
        self.put_mean = [float(i.get('put_mean', 0.0)) for i in data]
        self.put_forwardprice_eod = [float(i.get('put_forwardprice_eod', 0.0)) for i in data]
        self.expirationdate = [float(i.get('expirationdate', 0.0)) for i in data]
        self.call_forwardprice = [float(i.get('call_forwardprice', 0.0)) for i in data]
        self.call_gamma = [float(i.get('call_gamma', 0.0)) for i in data]
        self.put_alpha_eod = [float(i.get('put_alpha_eod', 0.0)) for i in data]
        self.put_delta = [float(i.get('put_delta', 0.0)) for i in data]
        self.put_openinterest_eod = [float(i.get('put_openinterest_eod', 0.0)) for i in data]
        self.put_gamma_eod = [float(i.get('put_gamma_eod', 0.0)) for i in data]
        self.call_changepercent_eod = [float(i.get('call_changepercent_eod', 0.0)) for i in data]
        self.put_bidsize = [float(i.get('put_bidsize', 0.0)) for i in data]
        self.call_vega_eod = [float(i.get('call_vega_eod', 0.0)) for i in data]
        self.put_rho_eod = [float(i.get('put_rho_eod', 0.0)) for i in data]
        self.put_alpha = [float(i.get('put_alpha', 0.0)) for i in data]
        self.call_theoprice = [float(i.get('call_theoprice', 0.0)) for i in data]

        # Creating data_dict with all attributes
        self.data_dict = {
            'call_change_eod': self.call_change_eod,
            'call_ivbid': self.call_ivbid,
            'call_iv_eod': self.call_iv_eod,
            'put_theta_eod': self.put_theta_eod,
            'call_ivask': self.call_ivask,
            'call_days': self.call_days,
            'call_mean_eod': self.call_mean_eod,
            'put_asksize': self.put_asksize,
            'call_ivint': self.call_ivint,
            'call_delta_eod': self.call_delta_eod,
            'call_bid_eod': self.call_bid_eod,
            'call_theoprice_eod': self.call_theoprice_eod,
            'put_iv': self.put_iv,
            'call_ivint_eod': self.call_ivint_eod,
            'call_ask_eod': self.call_ask_eod,
            'call_iv': self.call_iv,
            'put_days': self.put_days,
            'put_iv_eod': self.put_iv_eod,
            'put_change_eod': self.put_change_eod,
            'call_volume_eod': self.call_volume_eod,
            'call_ask': self.call_ask,
            'call_bidtime': self.call_bidtime,
            'call_rho': self.call_rho,
            'call_forwardprice_eod': self.call_forwardprice_eod,
            'call_mean': self.call_mean,
            'put_bid_eod': self.put_bid_eod,
            'call_bid': self.call_bid,
            'call_volume': self.call_volume,
            'call_alpha': self.call_alpha,
            'put_bidtime': self.put_bidtime,
            'call_vega': self.call_vega,
            'put_theta': self.put_theta,
            'put_optionsymbol': self.put_symbol,
            'put_ivask': self.put_ivask,
            'put_changepercent_eod': self.put_changepercent_eod,
            'put_ask': self.put_ask,
            'put_rho': self.put_rho,
            'call_openinterest_eod': self.call_openinterest_eod,
            'put_ivint': self.put_ivint,
            'put_theoprice': self.put_theoprice,
            'put_bid': self.put_bid,
            'call_asktime': self.call_asktime,
            'put_ask_eod': self.put_ask_eod,
            'call_gamma_eod': self.call_gamma_eod,
            'call_optionsymbol': self.call_symbol,
            'put_paramvolapercent_eod': self.put_paramvolapercent_eod,
            'put_volume': self.put_volume,
            'call_asksize': self.call_asksize,
            'call_alpha_eod': self.call_alpha_eod,
            'put_volume_eod': self.put_volume_eod,
            'put_ivbid': self.put_ivbid,
            'call_pos': self.call_pos,
            'put_delta_eod': self.put_delta_eod,
            'put_changepercent': self.put_changepercent,
            'put_mean_eod': self.put_mean_eod,
            'call_changepercent': self.call_changepercent,
            'put_asktime': self.put_asktime,
            'put_pos': self.put_pos,
            'put_theoprice_eod': self.put_theoprice_eod,
            'put_gamma': self.put_gamma,
            'call_days_eod': self.call_days_eod,
            'call_bidsize': self.call_bidsize,
            'call_delta': self.call_delta,
            'put_change': self.put_change,
            'call_paramvolapercent_eod': self.call_paramvolapercent_eod,
            'call_theta_eod': self.call_theta_eod,
            'call_change': self.call_change,
            'put_ivint_eod': self.put_ivint_eod,
            'put_vega': self.put_vega,
            'call_theta': self.call_theta,
            'put_days_eod': self.put_days_eod,
            'put_forwardprice': self.put_forwardprice,
            'call_rho_eod': self.call_rho_eod,
            'quotetime': self.quotetime,
            'put_vega_eod': self.put_vega_eod,
            'strike': self.strike,
            'put_mean': self.put_mean,
            'put_forwardprice_eod': self.put_forwardprice_eod,
            'expirationdate': self.expirationdate,
            'call_forwardprice': self.call_forwardprice,
            'call_gamma': self.call_gamma,
            'put_alpha_eod': self.put_alpha_eod,
            'put_delta': self.put_delta,
            'put_openinterest_eod': self.put_openinterest_eod,
            'put_gamma_eod': self.put_gamma_eod,
            'call_changepercent_eod': self.call_changepercent_eod,
            'put_bidsize': self.put_bidsize,
            'call_vega_eod': self.call_vega_eod,
            'put_rho_eod': self.put_rho_eod,
            'put_alpha': self.put_alpha,
            'call_theoprice': self.call_theoprice
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)





class OICOptionsMonitor:
    def __init__(self, data):
        # Initialize attributes from the 'data' dictionary
        self.call_change_eod = [i.get('call_change_eod', 0) for i in data]
        self.call_ivbid = [i.get('call_ivbid', 0) for i in data]
        self.call_iv_eod = [i.get('call_iv_eod', 0) for i in data]
        self.put_theta_eod = [i.get('put_theta_eod', 0) for i in data]
        self.call_ivask = [i.get('call_ivask', 0) for i in data]
        self.call_days = [i.get('call_days', 0) for i in data]
        self.call_mean_eod = [i.get('call_mean_eod', 0) for i in data]
        self.call_ivint = [i.get('call_ivint', 0) for i in data]
        self.put_asksize = [i.get('put_asksize', 0) for i in data]
        self.call_delta_eod = [i.get('call_delta_eod', 0) for i in data]
        self.call_bid_eod = [i.get('call_bid_eod', 0) for i in data]
        self.call_theoprice_eod = [i.get('call_theoprice_eod', 0) for i in data]
        self.put_iv = [i.get('put_iv', 0) for i in data]
        self.call_ivint_eod = [i.get('call_ivint_eod', 0) for i in data]
        self.call_ask_eod = [i.get('call_ask_eod', 0) for i in data]
        self.call_iv = [i.get('call_iv', 0) for i in data]
        self.put_days = [i.get('put_days', 0) for i in data]
        self.put_iv_eod = [i.get('put_iv_eod', 0) for i in data]
        self.call_volume_eod = [i.get('call_volume_eod', 0) for i in data]
        self.put_change_eod = [i.get('put_change_eod', 0) for i in data]
        self.call_ask = [i.get('call_ask', 0) for i in data]
        self.call_bidtime = [i.get('call_bidtime', 0) for i in data]
        self.call_rho = [i.get('call_rho', 0) for i in data]
        self.call_forwardprice_eod = [i.get('call_forwardprice_eod', 0) for i in data]
        self.call_mean = [i.get('call_mean', 0) for i in data]
        self.put_bid_eod = [i.get('put_bid_eod', 0) for i in data]
        self.call_bid = [i.get('call_bid', 0) for i in data]
        self.call_volume = [i.get('call_volume', 0) for i in data]
        self.call_alpha = [i.get('call_alpha', 0) for i in data]
        self.call_vega = [i.get('call_vega', 0) for i in data]
        self.put_bidtime = [i.get('put_bidtime', 0) for i in data]
        self.put_theta = [i.get('put_theta', 0) for i in data]
        self.put_symbol = ["O:" + i.get('put_optionsymbol', '').replace(' ', '') for i in data]
        self.put_ivask = [i.get('put_ivask', 0) for i in data]
        self.put_changepercent_eod = [i.get('put_changepercent_eod', 0) for i in data]
        self.put_ask = [i.get('put_ask', 0) for i in data]
        self.put_rho = [i.get('put_rho', 0) for i in data]
        self.call_openinterest_eod = [i.get('call_openinterest_eod', 0) for i in data]
        self.put_ivint = [i.get('put_ivint', 0) for i in data]
        self.put_theoprice = [i.get('put_theoprice', 0) for i in data]
        self.call_asktime = [i.get('call_asktime', 0) for i in data]
        self.put_bid = [i.get('put_bid', 0) for i in data]
        self.call_gamma_eod = [i.get('call_gamma_eod', 0) for i in data]
        self.put_ask_eod = [i.get('put_ask_eod', 0) for i in data]
        self.call_symbol = ["O:" + i.get('call_optionsymbol', '').replace(' ', '') for i in data]
        self.put_paramvolapercent_eod = [i.get('put_paramvolapercent_eod', 0) for i in data]
        self.call_asksize = [i.get('call_asksize', 0) for i in data]
        self.put_volume = [i.get('put_volume', 0) for i in data]
        self.call_alpha_eod = [i.get('call_alpha_eod', 0) for i in data]
        self.put_volume_eod = [i.get('put_volume_eod', 0) for i in data]
        self.put_ivbid = [i.get('put_ivbid', 0) for i in data]
        self.call_pos = [i.get('call_pos', 0) for i in data]
        self.put_delta_eod = [i.get('put_delta_eod', 0) for i in data]
        self.put_changepercent = [i.get('put_changepercent', 0) for i in data]
        self.put_mean_eod = [i.get('put_mean_eod', 0) for i in data]
        self.call_changepercent = [i.get('call_changepercent', 0) for i in data]
        self.put_asktime = [i.get('put_asktime', 0) for i in data]
        self.put_pos = [i.get('put_pos', 0) for i in data]
        self.put_theoprice_eod = [i.get('put_theoprice_eod', 0) for i in data]
        self.put_gamma = [i.get('put_gamma', 0) for i in data]
        self.call_days_eod = [i.get('call_days_eod', 0) for i in data]
        self.call_bidsize = [i.get('call_bidsize', 0) for i in data]
        self.call_delta = [i.get('call_delta', 0) for i in data]
        self.put_change = [i.get('put_change', 0) for i in data]
        self.call_paramvolapercent_eod = [i.get('call_paramvolapercent_eod', 0) for i in data]
        self.call_theta_eod = [i.get('call_theta_eod', 0) for i in data]
        self.call_change = [i.get('call_change', 0) for i in data]
        self.put_ivint_eod = [i.get('put_ivint_eod', 0) for i in data]
        self.call_theta = [i.get('call_theta', 0) for i in data]
        self.put_vega = [i.get('put_vega', 0) for i in data]
        self.put_days_eod = [i.get('put_days_eod', 0) for i in data]
        self.put_forwardprice = [i.get('put_forwardprice', 0) for i in data]
        self.call_rho_eod = [i.get('call_rho_eod', 0) for i in data]
        self.quotetime = [i.get('quotetime', 0) for i in data]
        self.put_vega_eod = [i.get('put_vega_eod', 0) for i in data]
        self.strike = [i.get('strike', 0) for i in data]
        self.put_mean = [i.get('put_mean', 0) for i in data]
        self.put_forwardprice_eod = [i.get('put_forwardprice_eod', 0) for i in data]
        self.expirationdate = [i.get('expirationdate', 0) for i in data]
        self.call_forwardprice = [i.get('call_forwardprice', 0) for i in data]
        self.call_gamma = [i.get('call_gamma', 0) for i in data]
        self.put_alpha_eod = [i.get('put_alpha_eod', 0) for i in data]
        self.put_delta = [i.get('put_delta', 0) for i in data]
        self.put_openinterest_eod = [i.get('put_openinterest_eod', 0) for i in data]
        self.call_changepercent_eod = [i.get('call_changepercent_eod', 0) for i in data]
        self.put_gamma_eod = [i.get('put_gamma_eod', 0) for i in data]
        self.put_bidsize = [i.get('put_bidsize', 0) for i in data]
        self.call_vega_eod = [i.get('call_vega_eod', 0) for i in data]
        self.put_rho_eod = [i.get('put_rho_eod', 0) for i in data]
        self.put_alpha = [i.get('put_alpha', 0) for i in data]
        self.call_theoprice = [i.get('call_theoprice', 0) for i in data]

        # Create a data dictionary for the class
        self.data_dict = {
            'call_change_eod': self.call_change_eod,
            'call_ivbid': self.call_ivbid,
            'call_iv_eod': self.call_iv_eod,
            'put_theta_eod': self.put_theta_eod,
            'call_ivask': self.call_ivask,
            'call_days': self.call_days,
            'call_mean_eod': self.call_mean_eod,
            'call_ivint': self.call_ivint,
            'put_asksize': self.put_asksize,
            'call_delta_eod': self.call_delta_eod,
            'call_bid_eod': self.call_bid_eod,
            'call_theoprice_eod': self.call_theoprice_eod,
            'put_iv': self.put_iv,
            'call_ivint_eod': self.call_ivint_eod,
            'call_ask_eod': self.call_ask_eod,
            'call_iv': self.call_iv,
            'put_days': self.put_days,
            'put_iv_eod': self.put_iv_eod,
            'call_volume_eod': self.call_volume_eod,
            'put_change_eod': self.put_change_eod,
            'call_ask': self.call_ask,
            'call_bidtime': self.call_bidtime,
            'call_rho': self.call_rho,
            'call_forwardprice_eod': self.call_forwardprice_eod,
            'call_mean': self.call_mean,
            'put_bid_eod': self.put_bid_eod,
            'call_bid': self.call_bid,
            'call_volume': self.call_volume,
            'call_alpha': self.call_alpha,
            'call_vega': self.call_vega,
            'put_bidtime': self.put_bidtime,
            'put_theta': self.put_theta,
            'put_symbol': self.put_symbol,
            'put_ivask': self.put_ivask,
            'put_changepercent_eod': self.put_changepercent_eod,
            'put_ask': self.put_ask,
            'put_rho': self.put_rho,
            'call_openinterest_eod': self.call_openinterest_eod,
            'put_ivint': self.put_ivint,
            'put_theoprice': self.put_theoprice,
            'call_asktime': self.call_asktime,
            'put_bid': self.put_bid,
            'call_gamma_eod': self.call_gamma_eod,
            'put_ask_eod': self.put_ask_eod,
            'call_symbol': self.call_symbol,
            'put_paramvolapercent_eod': self.put_paramvolapercent_eod,
            'call_asksize': self.call_asksize,
            'put_volume': self.put_volume,
            'call_alpha_eod': self.call_alpha_eod,
            'put_volume_eod': self.put_volume_eod,
            'put_ivbid': self.put_ivbid,
            'call_pos': self.call_pos,
            'put_delta_eod': self.put_delta_eod,
            'put_changepercent': self.put_changepercent,
            'put_mean_eod': self.put_mean_eod,
            'call_changepercent': self.call_changepercent,
            'put_asktime': self.put_asktime,
            'put_pos': self.put_pos,
            'put_theoprice_eod': self.put_theoprice_eod,
            'put_gamma': self.put_gamma,
            'call_days_eod': self.call_days_eod,
            'call_bidsize': self.call_bidsize,
            'call_delta': self.call_delta,
            'put_change': self.put_change,
            'call_paramvolapercent_eod': self.call_paramvolapercent_eod,
            'call_theta_eod': self.call_theta_eod,
            'call_change': self.call_change,
            'put_ivint_eod': self.put_ivint_eod,
            'call_theta': self.call_theta,
            'put_vega': self.put_vega,
            'put_days_eod': self.put_days_eod,
            'put_forwardprice': self.put_forwardprice,
            'call_rho_eod': self.call_rho_eod,
            'quotetime': self.quotetime,
            'put_vega_eod': self.put_vega_eod,
            'strike': self.strike,
            'put_mean': self.put_mean,
            'put_forwardprice_eod': self.put_forwardprice_eod,
            'expirationdate': self.expirationdate,
            'call_forwardprice': self.call_forwardprice,
            'call_gamma': self.call_gamma,
            'put_alpha_eod': self.put_alpha_eod,
            'put_delta': self.put_delta,
            'put_openinterest_eod': self.put_openinterest_eod,
            'call_changepercent_eod': self.call_changepercent_eod,
            'put_gamma_eod': self.put_gamma_eod,
            'put_bidsize': self.put_bidsize,
            'call_vega_eod': self.call_vega_eod,
            'put_rho_eod': self.put_rho_eod,
            'put_alpha': self.put_alpha,
            'call_theoprice': self.call_theoprice,
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class ProbabilityMetrics:
    def __init__(self, data):

        self.style = data.get('style')
        self.price = data.get('price')
        self.priceStrike = data.get('priceStrike')
        self.expDate = data.get('expDate')
        self.daysToExp = data.get('daysToExp')
        self.hours = data.get('hours')
        self.minutes = data.get('minutes')
        self.rate = data.get('rate')
        self.lastDividendDate = data.get('lastDividendDate')
        self.lastDividendAmount = data.get('lastDividendAmount')
        self.dividendFrequency = data.get('dividendFrequency')


class ExpiryDates:
    def __init__(self, data):

        self.expDate = [i.get('expDate') for i in data]
        self.daysToExp = [i.get('daysToExp') for i in data]
        self.hours = [i.get('hours') for i in data]
        self.minutes = [i.get('minutes') for i in data]

        

class VolaSnapshot:
    def __init__(self, data, ticker):
        self.tdate = data.get("tdate", None)
        self.stockId = data.get("stockId", None)
        self.hv10 = data.get("hv10", None)
        self.hv20 = data.get("hv20", None)
        self.hv30 = data.get("hv30", None)
        self.hv60 = data.get("hv60", None)
        self.hv90 = data.get("hv90", None)
        self.hv120 = data.get("hv120", None)
        self.hv150 = data.get("hv150", None)
        self.hv180 = data.get("hv180", None)


        self.data_dict = { 
            'date': self.tdate,
            'stock_id': self.stockId,
            'hv10': self.hv10,
            'hv20': self.hv20,
            'hv30': self.hv30,
            'hv60': self.hv60,
            'hv90': self.hv90,
            'hv120': self.hv120,
            'hv150': self.hv150,
            'hv180': self.hv180
        }

        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])
        self.as_dataframe['ticker'] = ticker

class AllCompBoundVola:
    def __init__(self, data, ticker):
        self.maxValue1 = data.get("maxValue1", None)
        self.maxValueDate1 = data.get("maxValueDate1", None)
        self.minValue1 = data.get("minValue1", None)
        self.minValueDate1 = data.get("minValueDate1", None)
        self.maxValue2 = data.get("maxValue2", None)
        self.maxValueDate2 = data.get("maxValueDate2", None)
        self.minValue2 = data.get("minValue2", None)
        self.minValueDate2 = data.get("minValueDate2", None)
        self.maxValue3 = data.get("maxValue3", None)
        self.maxValueDate3 = data.get("maxValueDate3", None)
        self.minValue3 = data.get("minValue3", None)
        self.minValueDate3 = data.get("minValueDate3", None)
        self.maxValue4 = data.get("maxValue4", None)
        self.maxValueDate4 = data.get("maxValueDate4", None)
        self.minValue4 = data.get("minValue4", None)
        self.minValueDate4 = data.get("minValueDate4", None)
        self.maxValue5 = data.get("maxValue5", None)
        self.maxValueDate5 = data.get("maxValueDate5", None)
        self.minValue5 = data.get("minValue5", None)
        self.minValueDate5 = data.get("minValueDate5", None)
        self.maxValue6 = data.get("maxValue6", None)
        self.maxValueDate6 = data.get("maxValueDate6", None)
        self.minValue6 = data.get("minValue6", None)
        self.minValueDate6 = data.get("minValueDate6", None)
        self.maxValue7 = data.get("maxValue7", None)
        self.maxValueDate7 = data.get("maxValueDate7", None)
        self.minValue7 = data.get("minValue7", None)
        self.minValueDate7 = data.get("minValueDate7", None)
        self.maxValue8 = data.get("maxValue8", None)
        self.maxValueDate8 = data.get("maxValueDate8", None)
        self.minValue8 = data.get("minValue8", None)
        self.minValueDate8 = data.get("minValueDate8", None)

        self.data_dict = {
            "max_value_1": data.get("maxValue1", None),
            "max_value_date_1": data.get("maxValueDate1", None),
            "min_value_1": data.get("minValue1", None),
            "min_value_date_1": data.get("minValueDate1", None),
            "max_value_2": data.get("maxValue2", None),
            "max_value_date_2": data.get("maxValueDate2", None),
            "min_value_2": data.get("minValue2", None),
            "min_value_date_2": data.get("minValueDate2", None),
            "max_value_3": data.get("maxValue3", None),
            "max_value_date_3": data.get("maxValueDate3", None),
            "min_value_3": data.get("minValue3", None),
            "min_value_date_3": data.get("minValueDate3", None),
            "max_value_4": data.get("maxValue4", None),
            "max_value_date_4": data.get("maxValueDate4", None),
            "min_value_4": data.get("minValue4", None),
            "min_value_date_4": data.get("minValueDate4", None),
            "max_value_5": data.get("maxValue5", None),
            "max_value_date_5": data.get("maxValueDate5", None),
            "min_value_5": data.get("minValue5", None),
            "min_value_date_5": data.get("minValueDate5", None),
            "max_value_6": data.get("maxValue6", None),
            "max_value_date_6": data.get("maxValueDate6", None),
            "min_value_6": data.get("minValue6", None),
            "min_value_date_6": data.get("minValueDate6", None),
            "max_value_7": data.get("maxValue7", None),
            "max_value_date_7": data.get("maxValueDate7", None),
            "min_value_7": data.get("minValue7", None),
            "min_value_date_7": data.get("minValueDate7", None),
            "max_value_8": data.get("maxValue8", None),
            "max_value_date_8": data.get("maxValueDate8", None),
            "min_value_8": data.get("minValue8", None),
            "min_value_date_8": data.get("minValueDate8", None)
        }


        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])
        self.as_dataframe['ticker'] = ticker

class HistoricIVX:
    def __init__(self, data, ticker):

        self.hv20 = [i.get('hv20') for i in data]
        self.ivx30 = [i.get('ivx30') for i in data]
        self.tdate = [i.get('tdate') for i in data]

        self.data_dict = { 
            'hv20': self.hv20,
            'ivx30': self.ivx30,
            'date': self.tdate
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)
        self.as_dataframe['ticker'] = ticker



class VolatilityScale:
    def __init__(self, data, ticker):

        self.tdate = [i.get('tdate') for i in data]
        self.price = [i.get('price') for i in data]
        self.volume = [i.get('volume') for i in data]

        self.data_dict = { 
            'date': self.tdate,
            'price': self.price,
            'volume': self.volume
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)
        self.as_dataframe['ticker'] = ticker