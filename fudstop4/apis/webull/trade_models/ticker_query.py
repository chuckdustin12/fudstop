import pandas as pd
import json
class WebullStockData:
    """A class representing stock data obtained from Webull.

    Attributes:
        web_name (str): The name of the stock.
        web_symb (str): The stock's symbol.
        web_exchange (str): The exchange code where the stock is traded.
        web_stock_close (float): The stock's closing price.
        last_earnings (str): The date of the stock's latest earnings report.
        web_stock_vol (int): The stock's trading volume.
        web_change_ratio (float): The stock's price change ratio.
        web_stock_open (float): The stock's opening price.
        web_stock_high (float): The stock's highest price.
        web_stock_low (float): The stock's lowest price.
        fifty_high (float): The stock's 52-week high price.
        avg_vol3m (float): The stock's average trading volume over the past 3 months.
        fifty_low (float): The stock's 52-week low price.
        avg_10d_vol (float): The stock's average trading volume over the past 10 days.
        outstanding_shares (int): The number of outstanding shares of the stock.
        total_shares (int): The total number of shares of the stock.
        estimated_earnings (str): The estimated date of the stock's next earnings report.
        web_vibrate_ratio (float): The stock's price fluctuation ratio.
    """
    def __init__(self,i):
        
        self.web_name = i.get("name", None) if not isinstance(i, list) else [d.get('name') for d in i]
        self.web_symb = i.get("symbol", None) if not isinstance(i, list) else [d.get('symbol') for d in i]
        self.web_exchange = i.get("disExchangeCode", None) if not isinstance(i, list) else [d.get('disExchangeCode') for d in i]
        self.web_stock_close =i.get("close", None) if not isinstance(i, list) else [d.get('close') for d in i]
        self.last_earnings = i.get('latestEarningsDate',None) if not isinstance(i, list) else [d.get('latestEarningsDate') for d in i]
        self.web_stock_vol =i.get("volume",None) if not isinstance(i, list) else [d.get('volume') for d in i]
        self.web_change_ratio = i.get("changeRatio", None) if not isinstance(i, list) else [d.get('changeRatio') for d in i]
        self.web_stock_open =i.get("open",None) if not isinstance(i, list) else [d.get('open') for d in i]
        self.web_stock_high =i.get("high", None) if not isinstance(i, list) else [d.get('high') for d in i]
        self.web_stock_low =i.get("low", None) if not isinstance(i, list) else [d.get('low') for d in i]
        self.fifty_high = i.get("fiftyTwoWkHigh", None) if not isinstance(i, list) else [d.get('fiftyTwoWkHigh') for d in i]
        self.avg_vol3m = i.get('avgVol3M') if not isinstance(i, list) else [d.get('avgVol3M') for d in i]
        self.fifty_low = i.get("fiftyTwoWkLow", None) if not isinstance(i, list) else [d.get('fiftyTwoWkLow') for d in i]
        self.avg_10d_vol = i.get("avgVol10D", None) if not isinstance(i, list) else [d.get('avgVol10D') for d in i]
        self.outstanding_shares = i.get('outstandingShares', None)if not isinstance(i, list) else [d.get('outstandingShares') for d in i]
        self.total_shares = i.get('totalShares', None) if not isinstance(i, list) else [d.get('totalShares') for d in i]

        try:
            self.estimated_earnings = i.get("nextEarningDay", None) if not isinstance(i, list) else [d.get('nextEarningDay') for d in i]
            self.web_vibrate_ratio = i.get('vibrateRatio', None) if not isinstance(i, list) else [d.get('vibrateRatio') for d in i]
        except KeyError:
            self.estimated_earnings = None
            self.web_vibrate_ratio = None


            self.data_dict = {
                'Company Name': i.get("name", None),
                'Symbol': i.get("symbol", None),
                'Exchange': i.get("disExchangeCode", None),
                'Close Price': i.get("close", None),
                'Latest Earnings': i.get('latestEarningsDate', None),
                'Volume': i.get("volume", None),
                'Change Ratio': i.get("changeRatio", None),
                'Open Price': i.get("open", None),
                'High Price': i.get("high", None),
                'Low Price': i.get("low", None),
                '52week High': i.get("fiftyTwoWkHigh", None),
                'Avg 3month Volume': i.get('avgVol3M', None),
                '52week Low': i.get("fiftyTwoWkLow", None),
                'Avg 10day Volume': self.r.get("avgVol10D", None),
                'Outstanding Shares': self.r.get('outstandingShares', None),
                'Total Shares': self.r.get('totalShares', None)
            }

            self.df = pd.DataFrame(self.data_dict, index=[0])



class MultiQuote:
    def __init__(self, datas):
        self.tickerId = [i.get('tickerId') for i in datas]
        self.exchangeId = [i.get('exchangeId') for i in datas]
        self.type = [i.get('type') for i in datas]
        self.secType = [i.get('secType') for i in datas]
        self.regionId = [i.get('regionId') for i in datas]
        self.regionCode = [i.get('regionCode') for i in datas]
        self.currencyId = [i.get('currencyId') for i in datas]
        self.name = [i.get('name') for i in datas]
        self.symbol = [i.get('symbol') for i in datas]
        self.disSymbol = [i.get('disSymbol') for i in datas]
        self.disExchangeCode = [i.get('disExchangeCode') for i in datas]
        self.exchangeCode = [i.get('exchangeCode') for i in datas]
        self.listStatus = [i.get('listStatus') for i in datas]
        self.template = [i.get('template') for i in datas]
        self.derivativeSupport = [i.get('derivativeSupport') for i in datas]
        self.isPTP = [i.get('isPTP') for i in datas]
        self.filingsSupport = [i.get('filingsSupport') for i in datas]
        self.futuresSupport = [i.get('futuresSupport') for i in datas]
        self.mkTradeTime = [i.get('mkTradeTime') for i in datas]
        self.tradeTime = [i.get('tradeTime') for i in datas]
        self.status = [i.get('status') for i in datas]
        self.close = [i.get('close') for i in datas]
        self.change = [i.get('change') for i in datas]
        self.changeRatio = [i.get('changeRatio') for i in datas]
        self.marketValue = [i.get('marketValue') for i in datas]
        self.volume = [i.get('volume') for i in datas]
        self.turnoverRate = [i.get('turnoverRate') for i in datas]
        self.overnight = [i.get('overnight') for i in datas]
        self.timeZone = [i.get('timeZone') for i in datas]
        self.tzName = [i.get('tzName') for i in datas]
        self.preClose = [i.get('preClose') for i in datas]
        self.open = [i.get('open') for i in datas]
        self.high = [i.get('high') for i in datas]
        self.low = [i.get('low') for i in datas]
        self.vibrateRatio = [i.get('vibrateRatio') for i in datas]
        self.avgVol10D = [i.get('avgVol10D') for i in datas]
        self.avgVol3M = [i.get('avgVol3M') for i in datas]
        self.negMarketValue = [i.get('negMarketValue') for i in datas]
        self.pe = [i.get('pe') for i in datas]
        self.forwardPe = [i.get('forwardPe') for i in datas]
        self.indicatedPe = [i.get('indicatedPe') for i in datas]
        self.peTtm = [i.get('peTtm') for i in datas]
        self.eps = [i.get('eps') for i in datas]
        self.epsTtm = [i.get('epsTtm') for i in datas]
        self.pb = [i.get('pb') for i in datas]
        self.totalShares = [i.get('totalShares') for i in datas]
        self.outstandingShares = [i.get('outstandingShares') for i in datas]
        self.fiftyTwoWkHigh = [i.get('fiftyTwoWkHigh') for i in datas]
        self.fiftyTwoWkLow = [i.get('fiftyTwoWkLow') for i in datas]
        self.dividend = [i.get('dividend') for i in datas]
        self.yield_ = [i.get('yield') for i in datas]
        self.currencyCode = [i.get('currencyCode') for i in datas]
        self.lotSize = [i.get('lotSize') for i in datas]
        self.latestDividendDate = [i.get('latestDividendDate') for i in datas]
        self.latestSplitDate = [i.get('latestSplitDate') for i in datas]
        self.latestEarningsDate = [i.get('latestEarningsDate') for i in datas]
        self.ps = [i.get('ps') for i in datas]
        self.bps = [i.get('bps') for i in datas]
        self.estimateEarningsDate = [i.get('estimateEarningsDate') for i in datas]
        self.tradeStatus = [i.get('tradeStatus') for i in datas]

        self.data_dict = {
            'ticker_id': self.tickerId,
            'exchange_id': self.exchangeId,
            'type': self.type,
            'sec_type': self.secType,
            'region_id': self.regionId,
            'region_code': self.regionCode,
            'currency_id': self.currencyId,
            'name': self.name,
            'symbol': self.symbol,
            'dis_symbol': self.disSymbol,
            'dis_exchange_code': self.disExchangeCode,
            'exchange_code': self.exchangeCode,
            'list_status': self.listStatus,
            'template': self.template,
            'derivative_support': self.derivativeSupport,
            'is_ptp': self.isPTP,
            'filings_support': self.filingsSupport,
            'futures_support': self.futuresSupport,
            'mk_trade_time': self.mkTradeTime,
            'trade_time': self.tradeTime,
            'status': self.status,
            'close': self.close,
            'change': self.change,
            'change_ratio': self.changeRatio,
            'market_value': self.marketValue,
            'volume': self.volume,
            'turnover_rate': self.turnoverRate,
            'overnight': self.overnight,
            'time_zone': self.timeZone,
            'tz_name': self.tzName,
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
            'fifty_two_wk_high': self.fiftyTwoWkHigh,
            'fifty_two_wk_low': self.fiftyTwoWkLow,
            'dividend': self.dividend,
            'yield': self.yield_,
            'currency_code': self.currencyCode,
            'lot_size': self.lotSize,
            'latest_dividend_date': self.latestDividendDate,
            'latest_split_date': self.latestSplitDate,
            'latest_earnings_date': self.latestEarningsDate,
            'ps': self.ps,
            'bps': self.bps,
            'estimate_earnings_date': self.estimateEarningsDate,
            'trade_status': self.tradeStatus
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)




        