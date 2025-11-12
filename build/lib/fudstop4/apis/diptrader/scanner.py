import pandas as pd
from fudstop4._markets.list_sets.ticker_lists import most_active_tickers


class ScanResults:
    def __init__(self, data):

        self.Ticker = [item.get('Ticker', None) for item in data]
        self.Description = [item.get('Description', None) for item in data]
        self.Exchange = [item.get('Exchange', None) for item in data]
        self.CurrentPrice = [item.get('CurrentPrice', None) for item in data]
        self.BidPrice = [item.get('BidPrice', None) for item in data]
        self.AskPrice = [item.get('AskPrice', None) for item in data]
        self.Spread = [item.get('Spread', None) for item in data]
        self.BidSize = [item.get('BidSize', None) for item in data]
        self.AskSize = [item.get('AskSize', None) for item in data]
        self.PreviousClose = [item.get('PreviousClose', None) for item in data]
        self.PreviousHigh = [item.get('PreviousHigh', None) for item in data]
        self.PreviousLow = [item.get('PreviousLow', None) for item in data]
        self.Open = [item.get('Open', None) for item in data]
        self.Vwap = [item.get('Vwap', None) for item in data]
        self.PercentFromVwap = [item.get('PercentFromVwap', None) for item in data]
        self.LatestVwapCross = [item.get('LatestVwapCross', None) for item in data]
        self.LatestHod = [item.get('LatestHod', None) for item in data]
        self.LatestLod = [item.get('LatestLod', None) for item in data]
        self.DailyHigh = [item.get('DailyHigh', None) for item in data]
        self.DailyLow = [item.get('DailyLow', None) for item in data]
        self.DailyVolume = [item.get('DailyVolume', None) for item in data]
        self.PercentChange = [item.get('PercentChange', None) for item in data]
        self.LastUpdated = [item.get('LastUpdated', None) for item in data]
        self.SharesOutstanding = [item.get('SharesOutstanding', None) for item in data]
        self.MarketCap = [item.get('MarketCap', None) for item in data]
        self.MarketCapFloat = [item.get('MarketCapFloat', None) for item in data]
        self.RelativeVolume = [item.get('RelativeVolume', None) for item in data]
        self.FiveMinRelativeVolumeChange = [item.get('FiveMinRelativeVolumeChange', None) for item in data]
        self.YesterdayVolume = [item.get('YesterdayVolume', None) for item in data]
        self.YesterdayVolumeString = [item.get('YesterdayVolumeString', None) for item in data]
        self.TrueRanges = [item.get('TrueRanges', None) for item in data]
        self.HaltReasonCode = [item.get('HaltReasonCode', None) for item in data]
        self.HaltResumptionTime = [item.get('HaltResumptionTime', None) for item in data]
        self.FiftyTwoWeekHigh = [item.get('FiftyTwoWeekHigh', None) for item in data]
        self.FiftyTwoWeekLow = [item.get('FiftyTwoWeekLow', None) for item in data]
        self.FiftyTwoWeekRange = [item.get('FiftyTwoWeekRange', None) for item in data]
        self.Volume1Min = [item.get('Volume1Min', None) for item in data]
        self.Volume1MinString = [item.get('Volume1MinString', None) for item in data]
        self.Volume1MinStringShort = [item.get('Volume1MinStringShort', None) for item in data]
        self.Volume5Min = [item.get('Volume5Min', None) for item in data]
        self.Volume5MinString = [item.get('Volume5MinString', None) for item in data]
        self.DailyVolumeString = [item.get('DailyVolumeString', None) for item in data]
        self.BidSizeString = [item.get('BidSizeString', None) for item in data]
        self.AskSizeString = [item.get('AskSizeString', None) for item in data]
        self.DailyCapital = [item.get('DailyCapital', None) for item in data]
        self.DailyCapitalString = [item.get('DailyCapitalString', None) for item in data]
        self.Capital5Min = [item.get('Capital5Min', None) for item in data]
        self.Capital5MinString = [item.get('Capital5MinString', None) for item in data]
        self.Capital1Min = [item.get('Capital1Min', None) for item in data]
        self.Capital1MinString = [item.get('Capital1MinString', None) for item in data]
        self.FloatRotation = [item.get('FloatRotation', None) for item in data]
        self.OneMinPercentChange = [item.get('OneMinPercentChange', None) for item in data]
        self.FiveMinPercentChange = [item.get('FiveMinPercentChange', None) for item in data]
        self.IntradayPercentChange = [item.get('IntradayPercentChange', None) for item in data]
        self.PercentOfAverageOneMinVolume = [item.get('PercentOfAverageOneMinVolume', None) for item in data]
        self.VolumeTrend = [item.get('VolumeTrend', None) for item in data]
        self.Volatility = [item.get('Volatility', None) for item in data]
        self.MarketCapVolatility = [item.get('MarketCapVolatility', None) for item in data]
        self.MarketCapString = [item.get('MarketCapString', None) for item in data]
        self.MarketCapFloatString = [item.get('MarketCapFloatString', None) for item in data]
        self.ATR = [item.get('ATR', None) for item in data]
        self.AtrString = [item.get('AtrString', None) for item in data]
        self.CurrentRange = [item.get('CurrentRange', None) for item in data]
        self.SharesOutstandingString = [item.get('SharesOutstandingString', None) for item in data]
        self.TrueRange = [item.get('TrueRange', None) for item in data]


        self.data_dict = {
            "ticker": self.Ticker,
            "description": self.Description,
            "exchange": self.Exchange,
            "current_price": self.CurrentPrice,
            "bid_price": self.BidPrice,
            "ask_price": self.AskPrice,
            "spread": self.Spread,
            "bid_size": self.BidSize,
            "ask_size": self.AskSize,
            "previous_close": self.PreviousClose,
            "previous_high": self.PreviousHigh,
            "previous_low": self.PreviousLow,
            "open": self.Open,
            "vwap": self.Vwap,
            "percent_from_vwap": self.PercentFromVwap,
            "latest_vwap_cross": self.LatestVwapCross,
            "latest_hod": self.LatestHod,
            "latest_lod": self.LatestLod,
            "daily_high": self.DailyHigh,
            "daily_low": self.DailyLow,
            "daily_volume": self.DailyVolume,
            "percent_change": self.PercentChange,
            "last_updated": self.LastUpdated,
            "shares_outstanding": self.SharesOutstanding,
            "market_cap": self.MarketCap,
            "market_cap_float": self.MarketCapFloat,
            "relative_volume": self.RelativeVolume,
            "five_min_relative_volume_change": self.FiveMinRelativeVolumeChange,
            "yesterday_volume": self.YesterdayVolume,
            "yesterday_volume_string": self.YesterdayVolumeString,
            "true_ranges": self.TrueRanges,
            "halt_reason_code": self.HaltReasonCode,
            "halt_resumption_time": self.HaltResumptionTime,
            "fifty_two_week_high": self.FiftyTwoWeekHigh,
            "fifty_two_week_low": self.FiftyTwoWeekLow,
            "fifty_two_week_range": self.FiftyTwoWeekRange,
            "volume_1min": self.Volume1Min,
            "volume_1min_string": self.Volume1MinString,
            "volume_1min_string_short": self.Volume1MinStringShort,
            "volume_5min": self.Volume5Min,
            "volume_5min_string": self.Volume5MinString,
            "daily_volume_string": self.DailyVolumeString,
            "bid_size_string": self.BidSizeString,
            "ask_size_string": self.AskSizeString,
            "daily_capital": self.DailyCapital,
            "daily_capital_string": self.DailyCapitalString,
            "capital_5min": self.Capital5Min,
            "capital_5min_string": self.Capital5MinString,
            "capital_1min": self.Capital1Min,
            "capital_1min_string": self.Capital1MinString,
            "float_rotation": self.FloatRotation,
            "one_min_percent_change": self.OneMinPercentChange,
            "five_min_percent_change": self.FiveMinPercentChange,
            "intraday_percent_change": self.IntradayPercentChange,
            "percent_of_average_one_min_volume": self.PercentOfAverageOneMinVolume,
            "volume_trend": self.VolumeTrend,
            "volatility": self.Volatility,
            "market_cap_volatility": self.MarketCapVolatility,
            "market_cap_string": self.MarketCapString,
            "market_cap_float_string": self.MarketCapFloatString,
            "atr": self.ATR,
            "atr_string": self.AtrString,
            "current_range": self.CurrentRange,
            "shares_outstanding_string": self.SharesOutstandingString,
            "true_range": self.TrueRange,
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)

        # Filter out rows where `ticker` is not in `most_active_tickers`
        most_active_set = set(most_active_tickers)
        self.as_dataframe = self.as_dataframe[self.as_dataframe["ticker"].isin(most_active_set)]