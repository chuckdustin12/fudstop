import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)

import os
import stocksera
import pandas as pd

from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from .models.daily_treasury import DailyTreasuryData
from .models.sec_filings import SECFilingsData
from .models.market_news import MarketNewsData
from .models.news_sentiment import NewsSentimentData
from .models.insider_trades import InsiderTrades
from .models.ftds import FailureToDeliverData
from .models.low_float import LowFloatData
from .models.highest_shorted import HighestShortedData
from .models.short_volume import ShortVolumeData
from .models.inflation import InflationData
from .models.borrowed_shares import BorrowedSharesData
from .models.jobless_claims import JoblessClaimsData
from .models.jim_cramer import JimCramerData
from .models.retail_sales import RetailSalesData
from .models.reverse_repo import ReverseRepoData
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

YOUR_STOCKSERA_KEY = os.environ.get('YOUR_STOCKSERA_KEY')
print(YOUR_STOCKSERA_KEY)
class StockSera:
    def __init__(self):


        self.client = stocksera.Client(YOUR_STOCKSERA_KEY)


        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')


    def borrowed_shares(self, ticker_or_tickers, concurrency=None) -> List[BorrowedSharesData]:
        def fetch_data(ticker):
            data = self.client.borrowed_shares(ticker)
            df = pd.DataFrame(data)
            df['ticker'] = df['ticker'].astype(str)
            df['fee'] = df['fee'].astype(float)
            df['available'] = df['available'].astype(int)
            df['date_updated'] = pd.to_datetime(df['date_updated'])
            df.columns = df.columns.str.lower()
            return df

        if concurrency and isinstance(ticker_or_tickers, list):
            data_frames = []
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = {executor.submit(fetch_data, ticker): ticker for ticker in ticker_or_tickers}

                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        data = future.result()
                    except Exception as exc:
                        print(f"{ticker} generated an exception: {exc}")
                    else:
                        print(data)
                        data_frames.append(data)

            combined_df = pd.concat(data_frames, ignore_index=True)
            return combined_df

        else:
            data = self.client.borrowed_shares(ticker_or_tickers)
            df = pd.DataFrame(data)
            df['ticker'] = df['ticker'].astype(str)
            df['fee'] = df['fee'].astype(float)
            df['available'] = df['available'].astype(int)
            df['date_updated'] = pd.to_datetime(df['date_updated'])
            df.columns = df.columns.str.lower()
            df.to_csv(f'data/stocksera/borrowed_shares_{ticker_or_tickers}.csv', index=False)  # Corrected the variable here
            return df
        

    def short_volume(self, ticker_or_tickers, date_from='2019-09-17', date_to='2023-10-27', concurrency=None) -> List[ShortVolumeData]:
        """
        REQUIRED:

        >>> ticker



        OPTIONAL: 
        >>> date_from: the date to start surveying
        >>> date_to: the date to stop surveying

        >>> concurrency: Run this command on a list of tickers rather than a single ticker.
        """
        
        def fetch_data(ticker):
            data = self.client.short_volume(ticker, date_from, date_to)
            df = pd.DataFrame(data)
            df.columns = df.columns.str.lower()
            df = df.rename({'short vol': 'short_vol', 'short exempt vol': 'short_exempt_vol', 'total vol': 'total_vol', '% shorted': 'percent_shorted'})
            df['date'] = pd.to_datetime(df['date'])
            df['ticker'] = ticker
            return df
        
        if concurrency and isinstance(ticker_or_tickers, list):
            data_frames = []
            
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = {executor.submit(fetch_data, ticker): ticker for ticker in ticker_or_tickers}
                
                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        data = future.result()
                    except Exception as exc:
                        print(f"{ticker} generated an exception: {exc}")
                    else:
                        data_frames.append(data)

            if data_frames:  # Check if list is not empty
                combined_df = pd.concat(data_frames, ignore_index=True)

                combined_df.columns = combined_df.columns.str.lower()
                print(combined_df.columns)
                combined_df = combined_df.rename({'short vol': 'short_vol', 'short exempt vol': 'short_exempt_vol', 'total vol': 'total_vol', '% shorted': 'percent_shorted'})
                combined_df['date'] = pd.to_datetime(combined_df['date'])

                return combined_df
            else:
                print("No data to concatenate.")
                return None
                
        else:
            return fetch_data(ticker_or_tickers)

    def daily_treasury(self, days: str = '100') -> List[DailyTreasuryData]:
        """
        Arguments:

        OPTIONAL:

        >>> days: number of days to survey
        
        """
        data = self.client.daily_treasury(days)
        df = pd.DataFrame(data)

        # Rename columns to standard underscore style
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("%", "percent").str.replace("?", "question")

        all_treasury_data = []
        for index, row in df.iterrows():
            daily_treasury_data = DailyTreasuryData(
                date=row['date'],
                close_balance=row['close_balance'],
                open_balance=row['open_balance'],
                amount_change=row['amount_change'],
                percent_change=row['percent_change'],
                moving_avg=row['moving_avg']
            )

            all_treasury_data.append(daily_treasury_data)

        return all_treasury_data
    


    def failure_to_deliver(self, ticker_or_tickers, date_from='2023-10-28', date_to='2024-01-01', concurrency=None) -> List[FailureToDeliverData]:
        """
        Arguments:

        required:

        >>> ticker or a list of tickers


        OPTIONAL:

        >>> concurrency: add concurrency when passing in a list
        
        """
        def fetch_data(ticker):
            data = self.client.ftd(ticker, date_from, date_to)
            df = pd.DataFrame(data)
            df['ticker'] = ticker  # Assuming the API doesn't return the ticker, add it manually
            df['date'] = pd.to_datetime(df['date'])
            df['ftd'] = df['ftd'].astype(float)
            df['price'] = df['price'].astype(float)
            df['ftd_x_$'] = df['ftd_x_$'].astype(float)
            df['t+35_date'] = pd.to_datetime(df['t+35_date'])
            df.columns = df.columns.str.lower()
            return df

        if concurrency and isinstance(ticker_or_tickers, list):
            data_frames = []
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = {executor.submit(fetch_data, ticker): ticker for ticker in ticker_or_tickers}

                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        data = future.result()
                    except Exception as exc:
                        print(f"{ticker} generated an exception: {exc}")
                    else:
                        data_frames.append(data)

            combined_df = pd.concat(data_frames, ignore_index=True)
            return combined_df

        else:
            data = self.client.ftd(ticker_or_tickers, date_from, date_to)
            df = pd.DataFrame(data)
            df['ticker'] = ticker_or_tickers  # Assuming the API doesn't return the ticker, add it manually
            df['date'] = pd.to_datetime(df['date'])
            df['ftd'] = df['ftd'].astype(float)
            df['price'] = df['price'].astype(float)
            df['ftd_x_$'] = df['ftd_x_$'].astype(float)
            df['t+35_date'] = pd.to_datetime(df['t+35_date'])
            df.columns = df.columns.str.lower()
            df.to_csv(f'data/stocksera/failure_to_deliver_{ticker_or_tickers}.csv', index=False)
            return df

    def highest_shorted(self) -> List[HighestShortedData]:
        """
        Arguments

        >>> None


        """

        data = self.client.short_interest()

        df = pd.DataFrame(data)
        df.columns = df.columns.str.lower()
      
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("%", "percent_")
        short_interest = []
        for i, row in df.iterrows():
            short_interest_data = HighestShortedData(rank=row['rank'], 
                                                     ticker=row['ticker'], 
                                                     date=row['date'], 
                                                     short_interest=row['short_interest'], 
                                                     average_volume= row['average_volume'],
                                                     days_to_cover=row['days_to_cover'],
                                                     percent_float_short=row['percent_float_short'])

            short_interest.append(short_interest_data)

        return short_interest
    def inflation(self, year:str=None):
        """
        Arguments:
        >>> year: OPTIONAL - the year to survey (default all results)
        """

        data = self.client.inflation()
        if year is None:
            data = InflationData(data)
            df = pd.DataFrame(data)
            df.columns = df.columns.str.lower()
            return df
        else:
            data = InflationData(data).get_inflation_by_year(year)
            df = pd.DataFrame(data, index=[year])
            df.columns = df.columns.str.lower()
            return df



    def jobless_claims(self, days:str='100', as_dataframe:bool=True) -> List[JoblessClaimsData]:
        """
        Arguments:

        >>> days: the number of days to survey (optional - default 100)
        >>> as_dataframe: return as a dataframe (optional -default False)
        """
        

        data = self.client.jobless_claims(days)
        formatted_data = [{k.lower().replace(' ', '_'): v for k, v in item.items()} for item in data]
        if as_dataframe == True:
            
            jobless_data_dicts = [JoblessClaimsData(**item).as_dict() for item in formatted_data]
       
            df = pd.DataFrame(jobless_data_dicts)
            df.columns = df.columns.str.lower()
            df = df[::-1]
            return df

        data = [JoblessClaimsData(**item) for item in formatted_data]

        return data
        



    
    def insider_trading(self, as_dataframe:bool=True) -> List[InsiderTrades]:
        """
        Arguments:

        >>> as_dataframe: optional - returns as a pandas dataframe. (default True)
        """
        
        data = self.client.insider_trading()
        formatted_data = [{k.lower().replace(' ', '_'): v for k, v in item.items()} for item in data]
        if as_dataframe == False:
            formatted_data = [InsiderTrades(**i) for i in formatted_data]
            return formatted_data

        df = pd.DataFrame(formatted_data)
        return df


  
    def jim_cramer(self, as_dataframe:bool=True) -> List[JimCramerData]:
        """
        Arguments:
        >>> as_dataframe: optional - returns as a pandas dataframe (default True)
        """

        data = self.client.jim_cramer()
        formatted_data = [{k.lower().replace(' ', '_'): v for k, v in item.items()} for item in data]
        if as_dataframe == False:
            formatted_data = [JimCramerData(**i) for i in formatted_data]
            return formatted_data
        df = pd.DataFrame(formatted_data)
        return df
       

   


    def low_float(self, as_dataframe: bool= True):
        """
        Arguments:

        >>> as_dataframe: optional - returns as a pandas dataframe (default True)
        """
        
        data = self.client.low_float()
        formatted_data = [{k.lower().replace(' ', '_'): v for k, v in item.items()} for item in data]

        if as_dataframe == False:
            formatted_data = LowFloatData(formatted_data)
            return formatted_data

        df = pd.DataFrame(formatted_data)
        return df
       



    def sec_filings(self, ticker_or_tickers, concurrency:str=None):
        """
        Arguments:

        >>> ticker_or_tickers: pass in a single ticker or list of tickers

        optional:

        >>> concurrency: if passing in a list - set concurrency levels
        """
        def fetch_data(ticker):
            data = self.client.sec_fillings(ticker)
            data = [{k.lower().replace(' ', '_'): v for k, v in item.items()} for item in data]
            df = pd.DataFrame(data)
            
            df['ticker'] = ticker
            df['filling'] = df['filling'].astype(str)
            df['description'] = df['description'].astype(str)
            df['filling_date'] = pd.to_datetime(df['filling_date'])
            df.columns = df.columns.str.lower()
            return df

        if concurrency and isinstance(ticker_or_tickers, list):
            data_frames = []
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = {executor.submit(fetch_data, ticker): ticker for ticker in ticker_or_tickers}

                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        data = future.result()
                    except Exception as exc:
                        print(f"{ticker} generated an exception: {exc}")
                    else:
                        data_frames.append(data)

            combined_df = pd.concat(data_frames, ignore_index=True)
            return combined_df

        else:
            data = self.client.sec_fillings(ticker_or_tickers)
            df = pd.DataFrame(data)
            df['ticker'] = ticker_or_tickers  # Assuming the API doesn't return the ticker, add it manually
            df['filling'] = df['filling'].astype(str)
            df['description'] = df['description'].astype(str)
            df['filling_date'] = pd.to_datetime(df['filling_date'])
            df.columns = df.columns.str.lower()
            df.to_csv(f'data/stocksera/sec_filings_{ticker_or_tickers}.csv', index=False)
            return df



 
    def news_sentiment(self, ticker_or_tickers, concurrency:str=None):
        """
        Arguments:

        required: 
        >>> ticker_or_tickers: pass in either a single ticker or list of tickers.

        optional:
        >>> if passing in a list of tickers, set concurrency level as needed.
        
        """
        def fetch_data(ticker):
            data = self.client.news_sentiment(ticker)
            df = pd.DataFrame(data)
            df.columns = df.columns.str.lower()
            df['ticker'] = ticker  # Assuming the API doesn't return the ticker, add it manually
        
            return df

        if concurrency and isinstance(ticker_or_tickers, list):
            data_frames = []
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = {executor.submit(fetch_data, ticker): ticker for ticker in ticker_or_tickers}

                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        data = future.result()
                        
                    except Exception as exc:
                        print(f"{ticker} generated an exception: {exc}")
                    else:
                        data_frames.append(data)

            combined_df = pd.concat(data_frames, ignore_index=True)
            return combined_df

        else:
            data = self.client.news_sentiment(ticker_or_tickers)
            df = pd.DataFrame(data)
            print(df.columns)
            df['ticker'] = ticker_or_tickers  # Assuming the API doesn't return the ticker, add it manually
            df['date'] = pd.to_datetime(df['date'])
            df['title'] = df['title'].astype(str)
            df['link'] = df['link'].astype(str)
            df['sentiment'] = df['sentiment'].astype(str)
            df.columns = df.columns.str.lower()
            df.to_csv(f'data/stocksera/news_sentiment_{ticker_or_tickers}.csv', index=False)
            return df
   


    def market_news(self, as_dataframe:bool=True) -> List[MarketNewsData]:
        """
        Arguments:

        >>> as_dataframe: optional - returns as a pandas dataframe (default True)
        
        """

        data = self.client.market_news()
        formatted_data = [{k.lower().replace(' ', '_'): v for k, v in item.items()} for item in data]

        if as_dataframe == False:
            formatted_data = [MarketNewsData(**i) for i in formatted_data]
            return formatted_data

        df = pd.DataFrame(formatted_data)
        

        return df


    def retail_sales(self, days:str='100', as_dataframe:bool=True) -> List[RetailSalesData]:

        """
        Arguments:

        >>> days: optional: the days to survey (default 100)
        >>> as_dataframe: optional - returns as a pandas dataframe (default true).
        """

        data = self.client.retail_sales(days)
        formatted_data = [{k.lower().replace(' ', '_'): v for k, v in item.items()} for item in data]
        if as_dataframe == False:
            formatted_data = [RetailSalesData(**i) for i in formatted_data]
            return formatted_data

        df = pd.DataFrame(formatted_data)
        df = df[::-1] #reverse the order
        return df
       



    def reverse_repo(self, days:str='100', as_dataframe: bool=True) -> List[ReverseRepoData]:
        """
        Arguments:
        >>> days: optional - the number of days to survey
        >>> as_dataframe: optional - returns as a pandas dataframe (default True)
        """

        data = self.client.reverse_repo(days)
        formatted_data = [{k.lower().replace(' ', '_'): v for k, v in item.items()} for item in data]

        if as_dataframe == False:
            formatted_data = [ReverseRepoData(**i) for i in formatted_data]
            return formatted_data

        df = pd.DataFrame(formatted_data)


        return df