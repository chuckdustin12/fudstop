import pandas as pd

class SpyData:
    def __init__(self, data):
        self.quoteDate = [i.get("quoteDate", None) for i in data]
        self.pOpen = [i.get("pOpen", None) for i in data]
        self.high = [i.get("high", None) for i in data]
        self.low = [i.get("low", None) for i in data]
        self.pClose = [i.get("pClose", None) for i in data]
        self.volume = [i.get("volume", None) for i in data]
        self.advance = [i.get("advance", None) for i in data]
        self.decline = [i.get("decline", None) for i in data]
        self.upVol = [i.get("upVol", None) for i in data]
        self.downVol = [i.get("downVol", None) for i in data]
        self.upMoney = [i.get("upMoney", None) for i in data]
        self.downMoney = [i.get("downMoney", None) for i in data]
        self.rollingEPS = [i.get("rollingEPS", None) for i in data]
        self.sentiment = [i.get("sentiment", None) for i in data]
        self.na = [i.get("na", None) for i in data]
        self.ai = [i.get("ai", None) for i in data]
        self.netSentRatio = [i.get("netSentRatio", None) for i in data]
        self.netBuyRatio = [i.get("netBuyRatio", None) for i in data]
        self.peakTroughShorts = [i.get("peakTroughShorts", None) for i in data]
        self.avg20Day = [i.get("avg20Day", None) for i in data]
        self.targetRatio = [i.get("targetRatio", None) for i in data]


        # Construct self.data_dict
        self.data_dict = {
            "quote_date": self.quoteDate,
            "pre_open": self.pOpen,
            "high": self.high,
            "low": self.low,
            "pre_lose": self.pClose,
            "volume": self.volume,
            "advance": self.advance,
            "decline": self.decline,
            "up_vol": self.upVol,
            "down_vol": self.downVol,
            "up_money": self.upMoney,
            "down_money": self.downMoney,
            "rolling_eps": self.rollingEPS,
            "sentiment": self.sentiment,
            "na": self.na,
            "ai": self.ai,
            "net_sentiment_ratio": self.netSentRatio,
            "net_buy_ratio": self.netBuyRatio,
            "peak_trough_shorts": self.peakTroughShorts,
            "avg_20_day": self.avg20Day,
            "target_ratio": self.targetRatio
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)
        self.as_dataframe = self.as_dataframe[::-1]

class TopSentimentHeatmap:
    def __init__(self, data):
        self.ticker = [i.get('ticker', None) for i in data]
        self.total = [i.get('total', None) for i in data]
        self.avg_sentiment = [i.get('avgSent', None) for i in data]
        self.company = [i.get('company', None) for i in data]
        self.next_eps_date = [i.get('nextEPSDate', None) for i in data]
        self.release_time = [i.get('releaseTime', None) for i in data]
        self.confirm_date = [i.get('confirmDate', None) for i in data]
        self.sentiment = [i.get('sentiment', None) for i in data]


        self.data_dict = { 

            'ticker': self.ticker,
            'total': self.total,
            'avg_sentiment': self.avg_sentiment,
            'company': self.company,
            'next_er': self.next_eps_date,
            'release_time': self.release_time,
            'confirmation_date': self.confirm_date,
            'sentiment': self.sentiment
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class UpcomingRussellAndSectors:
    def __init__(self, data):
        self.earningsDate = [i.get("earningsDate", None) for i in data]
        self.epsGrowth = [i.get("epsGrowth", None) for i in data]
        self.maxDate = [i.get("maxDate", None) for i in data]
        self.revGrowth = [i.get("revGrowth", None) for i in data]
        self.secID = [i.get("secID", None) for i in data]
        self.sector = [i.get("sector", None) for i in data]
        self.sectorName = [i.get("sectorName", None) for i in data]
        self.surprise = [i.get("surprise", None) for i in data]
        self.total = [i.get("total", None) for i in data]
        self.week = [i.get("week", None) for i in data]

        # Construct self.data_dict
        self.data_dict = {
            "earnings_date": self.earningsDate,
            "eps_growth": self.epsGrowth,
            "max_date": self.maxDate,
            "revenue_growth": self.revGrowth,
            "sector_id": self.secID,
            "sector": self.sector,
            "sector_name": self.sectorName,
            "surprise": self.surprise,
            "total": self.total,
            "week": self.week
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)




class DatedChartData:
    def __init__(self, data):
        self.quoteDate = [i.get("quoteDate", None) for i in data]
        self.ticker = [i.get("ticker", None) for i in data]
        self.name = [i.get("name", None) for i in data]
        self.pClose = [i.get("pClose", None) for i in data]
        self.pOpen = [i.get("pOpen", None) for i in data]
        self.high = [i.get("high", None) for i in data]
        self.low = [i.get("low", None) for i in data]
        self.volume = [i.get("volume", None) for i in data]
        self.fwdEPS = [i.get("fwdEPS", None) for i in data]
        self.arrow = [i.get("arrow", None) for i in data]
        self.shortShares = [i.get("shortShares", None) for i in data]
        self.sentiment = [i.get("sentiment", None) for i in data]
        self.ewUserNo = [i.get("ewUserNo", None) for i in data]
        self.traderStatus = [i.get("traderStatus", None) for i in data]
        self.investorStatus = [i.get("investorStatus", None) for i in data]
        self.epsOpenDate = [i.get("epsOpenDate", None) for i in data]
        self.avwap = [i.get("avwap", None) for i in data]

        # Construct self.data_dict
        self.data_dict = {
            "quote_date": self.quoteDate,
            "ticker": self.ticker,
            "name": self.name,
            "pre_close": self.pClose,
            "pre_open": self.pOpen,
            "high": self.high,
            "low": self.low,
            "volume": self.volume,
            "forward_eps": self.fwdEPS,
            "arrow": self.arrow,
            "short_shares": self.shortShares,
            "sentiment": self.sentiment,
            "ew_user_no": self.ewUserNo,
            "trader_status": self.traderStatus,
            "investor_status": self.investorStatus,
            "eps_open_date": self.epsOpenDate,
            "aggregate_vwap": self.avwap
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class Messages:
    def __init__(self, data):

        self.artDate = [i.get("artDate", None) for i in data]
        self.artNo = [i.get("artNo", None) for i in data]
        self.artType = [i.get("artType", None) for i in data]
        self.imageName = [i.get("imageName", None) for i in data]
        self.summary = [i.get("summary", None) for i in data]
        self.tickers = [i.get("tickers", None) for i in data]
        self.title = [i.get("title", None) for i in data]

        # Construct self.data_dict
        self.data_dict = {
            "art_date": self.artDate,
            "art_no": self.artNo,
            "art_type": self.artType,
            "image_name": self.imageName,
            "summary": self.summary,
            "tickers": self.tickers,
            "title": self.title
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class Pivots:
    def __init__(self, data):
        self.ticker = [i.get("ticker", None) for i in data]
        self.company = [i.get("company", None) for i in data]
        self.listType = [i.get("listType", None) for i in data]
        self.lastTrade = [i.get("lastTrade", None) for i in data]
        self.pivot_point = [i.get("pp", None) for i in data]
        self.resistance_1 = [i.get("r1", None) for i in data]
        self.support_1 = [i.get("s1", None) for i in data]
        self.resistance_2 = [i.get("r2", None) for i in data]
        self.support_2 = [i.get("s2", None) for i in data]
        self.resistance_3 = [i.get("r3", None) for i in data]
        self.support_3 = [i.get("s3", None) for i in data]

        # Construct self.data_dict
        self.data_dict = {
            "ticker": self.ticker,
            "company": self.company,
            "list_type": self.listType,
            "last_trade": self.lastTrade,
            "pivot_point": self.pivot_point,
            "resistance_1": self.resistance_1,
            "support_1": self.support_1,
            "resistance_2": self.resistance_2,
            "support_2": self.support_2,
            "resistance_3": self.resistance_3,
            "support_3": self.support_3
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)




class TodaysResults:
    def __init__(self, data):
        self.earningsGrowth = [i.get("earningsGrowth", None) for i in data]
        self.earningsSurprise = [i.get("earningsSurprise", None) for i in data]
        self.eps = [i.get("eps", None) for i in data]
        self.epsDate = [i.get("epsDate", None) for i in data]
        self.estimate = [i.get("estimate", None) for i in data]
        self.ewGrade = [i.get("ewGrade", None) for i in data]
        self.fileName = [i.get("fileName", None) for i in data]
        self.highEstimate = [i.get("highEstimate", None) for i in data]
        self.lowEstimate = [i.get("lowEstimate", None) for i in data]
        self.name = [i.get("name", None) for i in data]
        self.prevEarningsGrowth = [i.get("prevEarningsGrowth", None) for i in data]
        self.prevRevenueGrowth = [i.get("prevRevenueGrowth", None) for i in data]
        self.pwrRating = [i.get("pwrRating", None) for i in data]
        self.quarter = [i.get("quarter", None) for i in data]
        self.revenue = [i.get("revenue", None) for i in data]
        self.revenueEstimate = [i.get("revenueEstimate", None) for i in data]
        self.revenueGrowth = [i.get("revenueGrowth", None) for i in data]
        self.revenueSurprise = [i.get("revenueSurprise", None) for i in data]
        self.subject = [i.get("subject", None) for i in data]
        self.summary = [i.get("summary", None) for i in data]
        self.ticker = [i.get("ticker", None) for i in data]
        self.whisper = [i.get("whisper", None) for i in data]

        # Construct self.data_dict
        self.data_dict = {
            "earnings_growth": self.earningsGrowth,
            "earnings_surprise": self.earningsSurprise,
            "eps": self.eps,
            "er_date": self.epsDate,
            "estimate": self.estimate,
            "ew_grade": self.ewGrade,
            "file_name": self.fileName,
            "high_estimate": self.highEstimate,
            "low_estimate": self.lowEstimate,
            "name": self.name,
            "prev_earnings_growth": self.prevEarningsGrowth,
            "prev_revenue_growth": self.prevRevenueGrowth,
            "pwrRating": self.pwrRating,
            "quarter": self.quarter,
            "revenue": self.revenue,
            "revenue_estimate": self.revenueEstimate,
            "revenue_growth": self.revenueGrowth,
            "revenue_surprise": self.revenueSurprise,
            "subject": self.subject,
            "summary": self.summary,
            "ticker": self.ticker,
            "whisper": self.whisper
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)



class CalData:
    def __init__(self, data):

        self.company = [i.get("company", None) for i in data]
        self.confirmDate = [i.get("confirmDate", None) for i in data]
        self.epsTime = [i.get("epsTime", None) for i in data]
        self.nextEPSDate = [i.get("nextEPSDate", None) for i in data]
        self.q1EstEPS = [i.get("q1EstEPS", None) for i in data]
        self.q1RevEst = [i.get("q1RevEst", None) for i in data]
        self.qDate = [i.get("qDate", None) for i in data]
        self.qSales = [i.get("qSales", None) for i in data]
        self.quarterDate = [i.get("quarterDate", None) for i in data]
        self.releaseTime = [i.get("releaseTime", None) for i in data]
        self.ticker = [i.get("ticker", None) for i in data]
        self.total = [i.get("total", None) for i in data]

        # Construct self.data_dict
        self.data_dict = {
            "company": self.company,
            "confirmation_date": self.confirmDate,
            "earnings_time": self.epsTime,
            "next_er_date": self.nextEPSDate,
            "q1_estimate_eps": self.q1EstEPS,
            "q1_revenue_est": self.q1RevEst,
            "q_date": self.qDate,
            "q_sales": self.qSales,
            "quarter_date": self.quarterDate,
            "release_time": self.releaseTime,
            "ticker": self.ticker,
            "total": self.total
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)
    


class ChartData:
    def __init__(self, data):
        self.quoteDate = [i.get('quoteDate') for i in data]
        self.ticker = [i.get('ticker') for i in data]
        self.name = [i.get('name') for i in data]
        self.pClose = [i.get('pClose') for i in data]
        self.pOpen = [i.get('pOpen') for i in data]
        self.high = [i.get('high') for i in data]
        self.low = [i.get('low') for i in data]
        self.volume = [i.get('volume') for i in data]
        self.fwdEPS = [i.get('fwdEPS') for i in data]
        self.arrow = [i.get('arrow') for i in data]
        self.shortShares = [i.get('shortShares') for i in data]
        self.sentiment = [i.get('sentiment') for i in data]
        self.ewUserNo = [i.get('ewUserNo') for i in data]
        self.traderStatus = [i.get('traderStatus') for i in data]
        self.investorStatus = [i.get('investorStatus') for i in data]
        self.epsOpenDate = [i.get('epsOpenDate') for i in data]
        self.avwap = [i.get('avwap') for i in data]

        self.data_dict = { 
            'quote_date': self.quoteDate,
            'ticker': self.ticker,
            'name': self.name,
            'pre_close': self.pClose,
            'pre_open': self.pOpen,
            'high': self.high,
            'low': self.low,
            'volume': self.volume,
            'eps_date': self.epsOpenDate,
            'vwap': self.avwap,
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)