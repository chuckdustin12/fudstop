import pandas as pd
from typing import Optional, List, Dict, Any
class DailyTreasury:
    def __init__(self, data):
        self.date = [i.get('Date') if i.get('Date') is not None else None for i in data]
        self.close_balance = [i.get('Close Balance') if i.get('Close Balance') is not None else None for i in data]
        self.open_balance = [i.get('Open Balance') if i.get('Open Balance') is not None else None for i in data]
        self.amount_change = [i.get('Amount Change') if i.get('Amount Change') is not None else None for i in data]
        self.percent_change = [i.get('Percent Change') if i.get('Percent Change') is not None else None for i in data]
        self.moving_avg = [i.get('Moving Avg') if i.get('Moving Avg') is not None else None for i in data]

        self.data_dict = {
                'Date': self.date,
                'Close Balance': self.close_balance,
                'Open Balance': self.open_balance,
                'Amount Change': self.amount_change,
                'Percent Change': self.percent_change,
                'Moving Avg': self.moving_avg
            }
        self.as_dataframe = pd.DataFrame(self.data_dict)
        


class EarningsCalendar:
    def __init__(self, data):
        self.date = [i['date'] if i['date'] is not None else None for i in data]
        self.hour = [i['hour'] if i['hour'] is not None else None for i in data]
        self.ticker = [i['ticker'] if i['ticker'] is not None else None for i in data]
        self.eps_est = [i['eps_est'] if i['eps_est'] is not None else None for i in data]
        self.eps_act  = [i['eps_act'] if i['eps_act'] is not None else None for i in data]
        self.revenue_est = [i['revenue_est'] if i['revenue_est'] is not None else None for i in data]
        self.revenue_act = [i['revenue_act'] if i['revenue_act'] is not None else None for i in data]
        self.year = [i['year'] if i['year'] is not None else None for i in data]
        self.quarter = [i['quarter'] if i['quarter'] is not None else None for i in data]
        self.mkt_cap = [i['mkt_cap'] if i['mkt_cap'] is not None else None for i in data]


        self.data_dict = {
            'date': self.date,
            'hour': self.hour,
            'ticker': self.ticker,
            'eps_est': self.eps_est,
            'eps_act': self.eps_act,
            'revenue_est': self.revenue_est,
            'revenue_act': self.revenue_act,
            'year': self.year,
            'quarter': self.quarter,
            'mkt_cap': self.mkt_cap
             }
        self.as_dataframe = pd.DataFrame(self.data_dict)


class FTD:
    def __init__(self, data):
        print(data)
        self.date = [i['Date'] if i['Date'] is not None else None for i in data]
        try:
            self.ticker = [i['Ticker'] if i['Ticker'] is not None else None for i in data]
        except KeyError:
            self.ticker = "N/A"
        self.price = [i['Price'] if i['Price'] is not None else None for i in data]
        self.amount_ftd = [i['Failure to Deliver'] if 'Failure to Deliver' in i else None for i in data]

        self.t35_date = [i['T+35 Date'] if i['T+35 Date'] is not None else None for i in data]
        self.dollar_cost = [i['Amount (FTD x $)'] if 'Amount (FTD x $)' in i else None for i in data]

        self.data_dict = {
            'Date': self.date,
            'Ticker': self.ticker,
            'Price': self.price,
            'Failure to Deliver': self.amount_ftd,
            'T+35 Date': self.t35_date,
            'Amount (FTD x $)': self.dollar_cost
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)

class House:
    def __init__(self, data):
        house = data['house']
        self.trans_date = [i['Transaction Date'] if i['Transaction Date'] in i else None for i in house]
        self.owner = [i['Owner'] if i['Owner'] in i else None for i in house]
        self.ticker = [i['Ticker'] if i['Ticker'] in i else None for i in house]
        self.asset_desc = [i['Asset Description'] if i['Asset Description'] in i else None for i in house]
        self.asset_type = [i['Asset Type'] if i['Asset Type'] in i else None for i in house]
        self.type = [i['Type'] if i['Type'] in i else None for i in house]
        self.amount = [i['Amount'] if i['Amount'] in i else None for i in house]
        self.rep = [i['Representative'] if i['Representative'] in i else None for i in house]
        self.link = [i['Link'] if i['Link'] in i else None for i in house]
        self.disclosure_date = [i['Disclosure Date'] if i['Disclosure Date'] in i else None for i in house]
        self.district=[i['District'] if i['District'] in i else None for i in house]
        self.capital_gains_over_200k = [i['Cap Gains Over 200USD'] if i['Cap Gains Over 200USD'] in i else None for i in house]


class Inflation:
    def __init__(self, data):
        self.shares = data.get('shares')
        self.value = data.get('value')
        self.shares_total = data.get('shares_total')

        self.data_dict = { 

            'Shares': self.shares,
            'Value': self.value,
            'Shares Total': self.shares_total
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)

class IPOs:
    def __init__(self, data):
        self.date = [i['Date'] if i['Date'] is not None else None for i in data]
        self.exchange = [i['Exchange'] if i['Exchange'] is not None else None for i in data]
        self.expected_price = [i['Expected Price'] if i['Expected Price'] is not None else None for i in data]
        self.mkt_cap = [i['Mkt Cap'] if i['Mkt Cap'] is not None else None for i in data]
        self.name = [i['Name'] if i['Name'] is not None else None for i in data]
        self.number_shares = [i['Number Shares'] if i['Number Shares'] is not None else None for i in data]
        self.status = [i['Status'] if i['Status'] is not None else None for i in data]
        self.symbol = [i['Symbol'] if i['Symbol'] is not None else None for i in data]


        self.data_dict = {
            'Date': self.date,
            'Exchange': self.exchange,
            'Expected Price': self.expected_price,
            'Mkt Cap': self.mkt_cap,
            'Name': self.name,
            'Number Shares': self.number_shares,
            'Status': self.status,
            'Symbol': self.symbol
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)
class JimCramer:
    def __init__(self, data):
        self.ticker = [i['Ticker'] if i['Ticker'] is not None else None for i in data]
        self.date = [i['Date'] if i['Date'] is not None else None for i in data]
        self.segment = [i['Segment'] if i['Segment'] is not None else None for i in data]
        self.call = [i['Call'] if i['Call'] is not None else None for i in data]
        self.price = [i['Price'] if i['Price'] is not None else None for i in data]

        self.data_dict = {
            'Ticker': self.ticker,
            'Date': self.date,
            'Segment': self.segment,
            'Call': self.call,
            'Price': self.price
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)

class JoblessClaims:
    def __init__(self, data):
        self.date = [i['Date'] if i['Date'] is not None else None for i in data]
        self.number = [i['Number'] if i['Number'] is not None else None for i in data]
        self.percent_change = [i['Percent Change'] if i['Percent Change'] is not None else None for i in data]


        self.data_dict = {
            'Date': self.date,
            'Number': self.number,
            'Percent Change': self.percent_change
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)

class LowFloat:
    def __init__(self, data):
        self.Rank = [i['Rank'] if i['Rank'] is not None else None for i in data]
        self.ticker= [i['ticker'] if i['ticker'] is not None else None for i in data]
        self.company_name= [i['company_name'] if i['company_name'] is not None else None for i in data]
        self.exchange= [i['exchange'] if i['exchange'] is not None else None for i in data]
        self.previous_close= [i['previous_close'] if i['previous_close'] is not None else None for i in data]
        self.one_day_change= [i['one_day_change'] if i['one_day_change'] is not None else None for i in data]
        self.floating_shares= [i['floating_shares'] if i['floating_shares'] is not None else None for i in data]
        self.outstanding_shares= [i['outstanding_shares'] if i['outstanding_shares'] is not None else None for i in data]
        self.short_int= [i['short_int'] if i['short_int'] is not None else None for i in data]
        self.market_cap= [i['market_cap'] if i['market_cap'] is not None else None for i in data]
        self.industry= [i['industry'] if i['industry'] is not None else None for i in data]
        self.data_dict = {
            'Rank': self.Rank,
            'ticker': self.ticker,
            'company_name': self.company_name,
            'exchange': self.exchange,
            'previous_close': self.previous_close,
            'one_day_change': self.one_day_change,
            'floating_shares': self.floating_shares,
            'outstanding_shares': self.outstanding_shares,
            'short_int': self.short_int,
            'market_cap': self.market_cap,
            'industry': self.industry
            }
        
        self.as_dataframe = pd.DataFrame(self.data_dict)

class LatestInsiderTradingSummary:
    def __init__(self, data):
        self.ticker = [i['Ticker'] if i['Ticker'] is not None else None for i in data]
        self.amount = [i['Amount'] if i['Amount'] is not None else None for i in data]
        self.market_cap =[i['Market Cap'] if i['Market Cap'] is not None else None for i in data]
        self.percent_of_market_cap = [i['% of Mkt Cap'] if i['% of Mkt Cap'] is not None else None for i in data]
        self.data_dict = {
            'Ticker': self.ticker,
            'Amount': self.amount,
            'Market Cap': self.market_cap,
            '% of Mkt Cap': self.percent_of_market_cap
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)

class MarketNews:
    def __init__(self, data):
        self.Date = [i['Date'] if i['Date'] is not None else None for i in data]
        self.Title = [i['Title'] if i['Title'] is not None else None for i in data]
        self.Source = [i['Source'] if i['Source'] is not None else None for i in data]
        self.URL = [i['URL'] if i['URL'] is not None else None for i in data]
        self.Section = [i['Section'] if i['Section'] is not None else None for i in data]
        self.data_dict = {
            'Date': self.Date,
            'Title': self.Title,
            'Source': self.Source,
            'URL': self.URL,
            'Section': self.Section
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)

class MarketSummary:
    def __init__(self, data):
        self.snp500 = data.get('snp500')


class NewsSentiment:
    def __init__(self, data):
        self.Date = [i['Date'] if 'Date' in i else None for i in data]
        self.Title = [i['Title'] if 'Title' in i else None for i in data]
        self.Link = [i['Link'] if 'Link' in i else None for i in data]
        self.Sentiment = [i['Sentiment'] if 'Sentiment' in i else None for i in data]

class RetailSales:
    def __init__(self, data):
        self.date = [i['Date'] if i['Date'] is not None else None for i in data]
        self.amount = [i['Amount'] if i['Amount'] is not None else None for i in data]
        self.percent_change = [i['Percent Change'] if i['Percent Change'] is not None else None for i in data]
        self.monthly_avg_cases = [i['monthly_avg_cases'] if i['monthly_avg_cases'] is not None else None for i in data]

        self.data_dict = {
            'Date': self.date,
            'Amount': self.amount,
            'Percent Change': self.percent_change,
            'monthly_avg_cases': self.monthly_avg_cases
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)

class ReverseRepo:
    def __init__(self, data):
        self.date = [i['Date'] if i['Date'] is not None else None for i in data]
        self.amount = [i['Amount'] if i['Amount'] is not None else None for i in data]
        self.num_parties = [i['Num Parties'] if i['Num Parties'] is not None else None for i in data]
        self.average = [i['Average'] if i['Average'] is not None else None for i in data]
        self.moving_average = [i['Moving Average'] if i['Moving Average'] is not None else None for i in data]
        self.data_dict = {
            'Date': self.date,
            'Amount': self.amount,
            'Num Parties': self.num_parties,
            'Average': self.average,
            'Moving Average': self.moving_average
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)

class SECFillings:
    def __init__(self, data):
        self.Filling = [i['Filling'] if i['Filling'] is not None else None for i in data]
        self.Description = [i['Description'] if 'description' in i else None for i in data]
        self.FillingDate = [i['Filling Date'] if i['Filling Date'] is not None else None for i in data]
        self.report_url = [i['report_url'] if 'report_url' in i else None for i in data]
        self.filing_url = [i['filing_url'] if 'filing_url' in i else None for i in data]

        self.data_dict = {
            'Filling': self.Filling,
            'Description': self.Description,
            'Filling Date': self.FillingDate,
            'report_url': self.report_url,
            'filing_url': self.filing_url
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)
class Senate:

    def __init__(self, data):
        self.TransactionDate = [data.get('Transaction Date') for i in data]
        self.Owner = [data.get('Owner') if i['Owner'] is not None else None for i in data]
        self.Ticker = [i['Ticker'] if i['Ticker'] is not None else None for i in data]
        self.AssetDescription = [i['Asset Description'] if i['Asset Description'] is not None else None for i in data]
        self.AssetType = [data.get('Asset Type') if i['Asset Type'] is not None else None for i in data]
        self.Type = [data.get('Type') if i['Type'] is not None else None for i in data]
        self.Amount = [data.get('Amount') if i['Amount'] is not None else None for i in data]
        self.Senator = [data.get('Senator') if i['Senator'] is not None else None for i in data]
        self.Link = [data.get('Link') if i['Link'] is not None else None for i in data]
        self.DisclosureDate = [data.get('Disclosure Date') if i['Disclosure Date'] is not None else None for i in data]


class ShortInterest:
    def __init__(self, data):

        
        self.Rank = [i['Rank'] if i['Rank'] is not None else None for i in data]
        self.Ticker= [i['Ticker'] if i['Ticker'] is not None else None for i in data]
        self.Date=[i['Date'] if i['Date'] is not None else None for i in data]
        self.ShortInterest= [i['Short Interest'] if i['Short Interest'] is not None else None for i in data]
        self.AverageVolume= [i['Average Volume'] if i['Average Volume'] is not None else None for i in data]
        self.DaysToCover= [i['Days To Cover'] if i['Days To Cover'] is not None else None for i in data]
        self.FloatShort= [i['% Float Short'] if i['% Float Short'] is not None else None for i in data]

        self.data_dict = {
            'Rank': self.Rank,
            'Ticker': self.Ticker,
            'Date': self.Date,
            'Short Interest': self.ShortInterest,
            'Average Volume': self.AverageVolume,
            'Days To Cover': self.DaysToCover,
            '% Float Short': self.FloatShort
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)

class ShortVolume:
    def __init__(
        self,
        date: Optional[str] = None,
        shortVol: Optional[str] = None,
        shortExemptVol: Optional[str] = None,
        totalVol: Optional[str] = None,
        percentShorted: Optional[str] = None
    ):
        self.date = date
        self.shortVol = shortVol
        self.shortExemptVol = shortExemptVol
        self.totalVol = totalVol
        self.percentShorted = percentShorted

    def data_dict(self) -> Dict[str, Any]:
        return {
            'date': self.date,
            'shortVol': self.shortVol,
            'shortExemptVol': self.shortExemptVol,
            'totalVol': self.totalVol,
            'percentShorted': self.percentShorted
        }

    def data_dict(self) -> Dict[str, Any]:
        return {
            'date': self.date,
            'shortVol': self.shortVol,
            'shortExemptVol': self.shortExemptVol,
            'totalVol': self.totalVol,
            'percentShorted': self.percentShorted
        }

    @classmethod
    def from_dict(cls, data: List[Dict[str, Any]]) -> List['ShortVolume']:
        return [cls(
            date=item.get('Date'),
            shortVol=item.get('Short Vol'),
            shortExemptVol=item.get('Short Exempt Vol'),
            totalVol=item.get('Total Vol'),
            percentShorted=item.get('% Shorted')
        ) for item in data]

    def __iter__(self):
        return iter(self.data_dict().values())
class StockTwits:
    def __init__(self, data):
        self.rank = [i['rank'] if i['rank'] is not None else None for i in data]
        self.watchlist = [i['watchlist'] if i['watchlist'] is not None else None for i in data]
        self.date_updated = [i['date_updated'] if i['date_updated'] is not None else None for i in data]


        self.data_dict = { 

            'Rank': self.rank,
            'Watchlists': self.watchlist,
            'Date Updated': self.date_updated
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)

class Subreddit:
    def __init__(self, data):
        self.Date = [i['Date'] if i['Date'] is not None else None for i in data]
        self.subreddit = [i['subreddit'] if i['subreddit'] is not None else None for i in data]
        self.Redditors = [i['Redditors'] if i['Redditors'] is not None else None for i in data]
        self.Active = [i['Active'] if i['Active'] is not None else None for i in data]
        self.percActive = [i['% Active'] if i['% Active'] is not None else None for i in data]
        self.percGrowth = [i['% Growth'] if i['% Growth'] is not None else None for i in data]
        self.percPriceChange = [i['% Price Change'] if i['% Price Change'] is not None else None for i in data]


        self.data_dict = {
            'Date': self.Date,
            'subreddit': self.subreddit,
            'Redditors': self.Redditors,
            'Active': self.Active,
            '% Active': self.percActive,
            '% Growth': self.percGrowth,
            '% Price Change': self.percPriceChange
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)
class TradingHalts:
    def __init__(self, data):
        self.halt_date = data.get('halt_date')
        self.halt_time = data.get('halt_time')
        self.ticker = data.get('ticker')
        self.exchange = data.get('exchange')
        self.reason = data.get('reason')
        self.resume_date = data.get('resume_date')
        self.resume_time = data.get('resume_time')

        self.data_dict = {
            'halt_date': self.halt_date,
            'halt_time': self.halt_time,
            'ticker': self.ticker,
            'exchange': self.exchange,
            'reason': self.reason,
            'resume_date': self.resume_date,
            'resume_time': self.resume_time
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)
class WSBMentions:
    def __init__(self, data):
        self.mentions = [i['mentions'] if i['mentions'] is not None else None for i in data]


class WSBOptions:
    def __init__(self, data):
        self.ticker = [i['Ticker'] if i['Ticker'] is not None else None for i in data]
        self.calls = [i['Calls'] if i['Calls'] is not None else None for i in data]
        self.puts = [i['Puts'] if i['Puts'] is not None else None for i in data]
        self.ratio = [i['Ratio'] if i['Ratio'] is not None else None for i in data]

        self.data_dict = {
            'Ticker': self.ticker,
            'Calls': self.calls,
            'Puts': self.puts,
            'Ratio': self.ratio
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)
class Insiders:
    def __init__(self, data):


        self.Ticker = [i['Ticker'] if i['Ticker'] is not None else None for i in data]
        self.Name = [i['Name'] if i['Name'] is not None else None for i in data]
        self.Relationship = [i['Relationship'] if i['Relationship'] is not None else None for i in data]
        self.Date = [i['Date'] if i['Date'] is not None else None for i in data]
        self.Transaction = [i['Transaction'] if i['Transaction'] is not None else None for i in data]
        self.Cost = [i['Cost'] if i['Cost'] is not None else None for i in data]
        self.Shares = [i['Shares'] if i['Shares'] is not None else None for i in data]
        self.Value = [i['Value ($)'] if i['Value ($)'] is not None else None for i in data]
        self.SharesTotal= [i['#Shares Total'] if i['#Shares Total'] is not None else None for i in data]
        self.DateFilled= [i['Date Filled'] if i['Date Filled'] is not None else None for i in data]



        self.data_dict = {
            'Ticker': self.Ticker,
            'Name': self.Name,
            'Relationship': self.Relationship,
            'Date': self.Date,
            'Transaction': self.Transaction,
            'Cost': self.Cost,
            'Shares': self.Shares,
            'Value ($)': self.Value,
            '#Shares Total': self.SharesTotal,
            'Date Filled': self.DateFilled
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)