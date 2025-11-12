#!/usr/bin/env python3
"""
yfSDK - A production-ready module for fetching financial data and inserting it into your database.

DATABASE CONNECTION AND USAGE INSTRUCTIONS:
    - This class uses a PostgreSQL database for storing fetched financial data.
    - Ensure you have PostgreSQL installed and running.
    - Default connection parameters in the __init__ method are:
          host:     'localhost'
          port:     5432 #default
          user:     'YOUR USER'
          password: 'YOUR PASSWORD'
          database: 'YOUR DATABASE'
    - If your database credentials differ, update these parameters in the __init__ method.
    - The database connection is managed asynchronously via the PolygonOptions class (which uses asyncpg).
    - Each async method calls `await self.db.connect()` before performing insert operations.
    - Make sure the required tables (e.g., 'balance_sheet', 'cash_flow', etc.) exist and their schemas match the DataFrame structure.
    - Install required dependencies (e.g., asyncpg) using: `pip install asyncpg`
    - To use any async method, ensure you're running them within an asyncio event loop.
"""
from fudstop4.apis.helpers import camel_to_snake, is_etf
import logging
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from ..._markets.list_sets.ticker_lists import all_tickers
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from ..helpers import lowercase_columns, format_large_numbers_in_dataframe


class YfSDK:
    """
    YfSDK provides methods to fetch financial data using yfinance and store it into a PostgreSQL database asynchronously.

    DATABASE CONNECTION AND USAGE INSTRUCTIONS:
        - The database connection is established using the PolygonOptions class, which leverages asyncpg.
        - Default Database Credentials:
              host:     'localhost'
              port:     5432
              user:     'chuck'
              password: 'fud'
              database: 'fudstop3'
        - Ensure your PostgreSQL instance is running with these credentials or update them as needed.
        - Every async method that interacts with the database calls `await self.db.connect()` before data insertion.
        - Confirm that the target tables (e.g., 'balance_sheet', 'cash_flow', etc.) exist and are structured appropriately.
        - Use this class by instantiating it and calling its async methods within an asyncio event loop.
    """

    def __init__(self):
        # Configure the list of tickers and initialize the database connection using PolygonOptions.
        self.tickers = all_tickers
        self.db = PolygonOptions(
            database='fudstop3',
            host='localhost',
            user='chuck',
            password='fud',
            port=5432
        )

    async def balance_sheet(
        self, ticker: str, frequency: str = 'quarterly', pretty: bool = False, as_dict: bool = False
    ):
        """
        Gets balance sheet information for a ticker and inserts it into the database.

        Arguments:
            ticker (str): The ticker symbol.
            frequency (str): 'quarterly' or 'annual' (default 'quarterly').
            pretty (bool): Option to pretty-print the output (default False).
            as_dict (bool): Return data as a dictionary (default False).

        Database Usage:
            - Transposes and processes the data.
            - Establishes a connection with `await self.db.connect()`.
            - Inserts data into the 'balance_sheet' table using a unique constraint on 'ticker'.
        """
        data = yf.Ticker(ticker)
        balance_sheet = data.get_balance_sheet(freq=frequency, pretty=pretty, as_dict=as_dict)

        df = balance_sheet.transpose()
        df = lowercase_columns(df)
        df['ticker'] = ticker

        return df

    async def get_cash_flow(
        self, ticker: str, frequency: str = 'quarterly', pretty: bool = False, as_dict: bool = False
    ):
        """
        Gets cash flow information for a ticker and inserts it into the database.

        Arguments:
            ticker (str): The ticker symbol.
            frequency (str): 'quarterly' or 'annual' (default 'quarterly').
            pretty (bool): Option to pretty-print the output (default False).
            as_dict (bool): Return data as a dictionary (default False).

        Database Usage:
            - Processes and transposes the cash flow data.
            - Establishes a connection using `await self.db.connect()`.
            - Inserts data into the 'cash_flow' table with 'ticker' as a unique column.
        """
        data = yf.Ticker(ticker).get_cash_flow(freq=frequency, pretty=pretty, as_dict=as_dict)
        logger.info("Cash flow data columns: %s", data.columns)


        df = data.transpose()
        df = lowercase_columns(df)
        df['ticker'] = ticker

        return df

    async def get_all_candles(self, tickers: str):
        """
        Gets OHLC, adjusted close, and volume data for all dates for the provided tickers.

        Arguments:
            tickers (str): A comma-separated list of ticker symbols.

        Database Usage:
            - Optionally, the returned DataFrame can be inserted into a 'candles' table.
            - Establishes a connection with `await self.db.connect()` before processing data.
        """
        try:
            chart_data = yf.download(tickers)
            chart_data = pd.DataFrame(chart_data).reset_index()
            df = lowercase_columns(chart_data)
            df['ticker'] = tickers
            return chart_data
        except Exception as e:
            logger.error("Error processing candle data for %s: %s", tickers, e)
            return pd.DataFrame()

    async def dividends(self, ticker: str):
        """
        Returns historic dividends for a ticker.

        Arguments:
            ticker (str): The ticker symbol.

        Returns:
            Dividend data or an error message.
        """
        try:
            data = yf.Ticker(ticker).get_dividends()
            return data
        except Exception as e:
            logger.error("No dividends found for %s: %s", ticker, e)
            return f"No dividends found for {ticker}. {e}"


    async def fast_info(self, ticker: str):
        """
        Returns fast info for a ticker as a single-row DataFrame where 
        the dictionary keys become the column names.
        """
        # Get the fast info from yfinance.
        info = yf.Ticker(ticker).get_fast_info()  # this is a lazy-loading dict

        # Convert it to a standard dictionary.
        # Option 1: Using dict() on info.items()
        info_dict = dict(info.items())
        # Option 2 (if Option 1 doesn't work reliably):
        # info_dict = {key: info[key] for key in info.keys()}

        # Create a DataFrame with one row by wrapping the dictionary in a list.
        df = pd.DataFrame([info_dict])
        df.columns = [camel_to_snake(col) for col in df.columns]
        return df


    async def financials(self, ticker: str, frequency: str = 'quarterly', as_dict: bool = False, pretty: bool = False):
        """
        Gets all financials for a ticker.

        Arguments:
            ticker (str): The ticker symbol.
            frequency (str): 'quarterly' or 'annual' (default 'quarterly').
            as_dict (bool): Return data as a dictionary (default False).
            pretty (bool): Option to pretty-print the output (default False).

        Returns:
            A formatted DataFrame containing financial data.
        """
        data = yf.Ticker(ticker).get_financials(freq=frequency, as_dict=as_dict, pretty=pretty)
        formatted_data = format_large_numbers_in_dataframe(data)
        return formatted_data

    async def income_statement(
        self, ticker: str, frequency: str = 'quarterly', as_dict: bool = False, pretty: bool = False
    ):
        """
        Gets the income statement for a ticker and inserts it into the database.

        Arguments:
            ticker (str): The ticker symbol.
            frequency (str): 'quarterly' or 'annual' (default 'quarterly').
            as_dict (bool): Return data as a dictionary (default False).
            pretty (bool): Option to pretty-print the output (default False).

        Database Usage:
            - Transposes and cleans the income statement data.
            - Connects to the database with `await self.db.connect()`.
            - Inserts data into the 'income_statement' table using a unique 'ticker' column.
        """
        data = yf.Ticker(ticker).get_income_stmt(freq=frequency, as_dict=as_dict, pretty=pretty)
        await self.db.connect()
        df = data.transpose()
        df = lowercase_columns(df)
        df['ticker'] = ticker

        await self.db.batch_insert_dataframe(df, table_name='income_statement', unique_columns='ticker')
        formatted_data = format_large_numbers_in_dataframe(df)
        return formatted_data

    async def get_info(self, ticker: str):
        """
        Returns a large dictionary of information for a ticker and inserts it into the database.

        Arguments:
            ticker (str): The ticker symbol.

        Database Usage:
            - Converts the fetched info into a DataFrame.
            - Cleans and processes the data before inserting into the 'info' table.
            - Uses `await self.db.connect()` to establish a connection prior to insertion.
        """
        data = yf.Ticker(ticker).get_info()
        df = pd.DataFrame([data])
        await self.db.connect()
        df = lowercase_columns(df)
        df['ticker'] = ticker
        if 'companyofficers' in df.columns:
            df = df.drop(columns=['companyofficers'])
        await self.db.batch_insert_dataframe(df, table_name='info', unique_columns='ticker')
        formatted_data = format_large_numbers_in_dataframe(df)
        return formatted_data

    async def institutional_holdings(self, ticker: str):
        """
        Gets institutional holdings for a ticker and inserts them into the database.

        Arguments:
            ticker (str): The ticker symbol.

        Database Usage:
            - Processes and cleans the institutional holdings data.
            - Connects to the database using `await self.db.connect()`.
            - Inserts the data into the 'institutions' table, with 'ticker' as the unique column.
        """
        data = yf.Ticker(ticker).get_institutional_holders()
        await self.db.connect()
        df = lowercase_columns(data)
        df['ticker'] = ticker

        await self.db.batch_insert_dataframe(df, table_name='institutions', unique_columns='ticker')
        formatted_data = format_large_numbers_in_dataframe(df)
        if '% out' in formatted_data.columns:
            formatted_data['% out'] = formatted_data['% out'].astype(float).round(3)
        formatted_data.set_index('date reported', inplace=True)
        return formatted_data

    async def mutual_fund_holders(self, ticker: str):
        """
        Gets mutual fund holders for a ticker and inserts them into the database.

        Arguments:
            ticker (str): The ticker symbol.

        Database Usage:
            - Cleans and processes the mutual fund holders data.
            - Uses `await self.db.connect()` to ensure a database connection.
            - Inserts the data into the 'mf_holders' table.
        """
        data = yf.Ticker(ticker).get_mutualfund_holders()
        await self.db.connect()
        df = lowercase_columns(data)
        df['ticker'] = ticker

        await self.db.batch_insert_dataframe(df, table_name='mf_holders', unique_columns='ticker')
        formatted_data = format_large_numbers_in_dataframe(df)
        if '% out' in formatted_data.columns:
            formatted_data['% out'] = formatted_data['% out'].astype(float).round(3)
        formatted_data.set_index('date reported', inplace=True)
        return formatted_data

    async def atm_calls(self, ticker: str):
        """
        Gets at-the-money call options for a ticker and inserts them into the database.

        Arguments:
            ticker (str): The ticker symbol.

        Database Usage:
            - Processes the call options data.
            - Establishes a database connection using `await self.db.connect()`.
            - Inserts the data into the 'atm_calls' table using 'option_symbol' as the unique column.
        """
        calls_data = yf.Ticker(ticker)._download_options()
        call_options = calls_data.get('calls', [])
        df = pd.DataFrame(call_options)

        await self.db.connect()
        df = lowercase_columns(df)
        df['ticker'] = ticker
        if 'inthemoney' in df.columns:
            df['inthemoney'] = df['inthemoney'].astype(bool)
        df = df.rename(columns={'contractsymbol': "option_symbol"})
        await self.db.batch_insert_dataframe(df, table_name='atm_calls', unique_columns='option_symbol')
        return df

    async def atm_puts(self, ticker: str):
        """
        Gets at-the-money put options for a ticker and inserts them into the database.

        Arguments:
            ticker (str): The ticker symbol.

        Database Usage:
            - Processes the put options data.
            - Establishes a database connection via `await self.db.connect()`.
            - Inserts the data into the 'atm_puts' table with 'option_symbol' as the unique column.
        """
        puts_data = yf.Ticker(ticker)._download_options()
        put_options = puts_data.get('puts', [])
        df = pd.DataFrame(put_options)

        await self.db.connect()
        df = lowercase_columns(df)
        df['ticker'] = ticker
        if 'inthemoney' in df.columns:
            df['inthemoney'] = df['inthemoney'].astype(bool)
        df = df.rename(columns={'contractsymbol': "option_symbol"})
        await self.db.batch_insert_dataframe(df, table_name='atm_puts', unique_columns='option_symbol')
        return df



    async def analyst_price_targets(self, ticker):

        analysts = yf.Ticker(ticker)

        analysts = analysts.analyst_price_targets
        
        df = pd.DataFrame(analysts, index=[0])
        df['ticker'] = ticker
        df.columns = [camel_to_snake(col) for col in df.columns]
        df.reset_index(inplace=True)
        return df


    async def splits_and_dividends(self, ticker):

        sad = yf.Ticker(ticker)
        sad = sad.actions

        sad['ticker'] = ticker
        sad.columns = [camel_to_snake(col) for col in sad.columns]
        sad.reset_index(inplace=True)
        return sad
    

    async def calendar(self, ticker):

        data = yf.Ticker(ticker)

        data = data.calendar
        df = pd.DataFrame(data)
        df['ticker'] = ticker
        df.columns = [camel_to_snake(col) for col in df.columns]
        return df[::-1].head(1)


    async def earnings_estimates(self, ticker):

        data = yf.Ticker(ticker)

        data = data.earnings_estimate
        data['ticker'] = ticker
        data.columns = [camel_to_snake(col) for col in data.columns]
        data.reset_index(inplace=True)
        return data

    async def eps_trend(self, ticker):

        data = yf.Ticker(ticker)

        data = data.eps_trend
        data['ticker'] = ticker
        data.columns = [camel_to_snake(col) for col in data.columns]
        data.reset_index(inplace=True)
        return data

    async def eps_revisions(self, ticker):

        data = yf.Ticker(ticker)

        data = data.eps_revisions
        data['ticker'] = ticker
        data.columns = [camel_to_snake(col) for col in data.columns]
        data.reset_index(inplace=True)
        return data
    
    async def growth_estimates(self, ticker):

        data = yf.Ticker(ticker)

        data = data.growth_estimates
        data['ticker'] = ticker
        data.columns = [camel_to_snake(col) for col in data.columns]
        data.reset_index(inplace=True)
        return data

    async def insider_purchases(self, ticker):

        data = yf.Ticker(ticker)

        data = data.insider_purchases
        data['ticker'] = ticker
        data.columns = [camel_to_snake(col) for col in data.columns]
        return data
    

    async def executive_holdings(self, ticker):

        data = yf.Ticker(ticker)

        data = data.insider_roster_holders
        data['ticker'] = ticker
        data.columns = [camel_to_snake(col) for col in data.columns]
        return data


    async def insider_transactions(self, ticker):

        data = yf.Ticker(ticker)

        data = data.insider_transactions
        data['ticker'] = ticker
        data.columns = [camel_to_snake(col) for col in data.columns]
        return data
    

    async def institutional_holders(self, ticker):

        data = yf.Ticker(ticker)

        data = data.institutional_holders
        data['ticker'] = ticker
        data.columns = [camel_to_snake(col) for col in data.columns]
        return data

    async def major_holders(self, ticker):

        data = yf.Ticker(ticker)
        index = [
            'insidersPercentHeld',
            'institutionsPercentHeld',
            'institutionsFloatPercentHeld',
            'institutionsCount'
        ]
        data = data.major_holders
        data['ticker'] = ticker
        data.columns = [camel_to_snake(col) for col in data.columns]
        df = pd.DataFrame(data, index=index)
        df_reset = df.reset_index().rename(columns={'index': 'metric'})
        return df_reset
    

    async def mutual_fund_holders(self, ticker):

        data = yf.Ticker(ticker)
        data = data.mutualfund_holders
        data['ticker'] = ticker
        return data

    async def news(self, ticker):

        data = yf.Ticker(ticker)
        data = data.news
        content = [i.get('content') for i in data]
        title = [i.get('title') for i in content]
        description = [i.get('description') for i in content]
        summary = [i.get('summary') for i in content]
        pubDate = [i.get('pubDate') for i in content]
        displayTime = [i.get('displayTime') for i in content]



        thumbnail = [i.get('thumbnail') for i in content]
        provider = [i.get('provider') for i in content]
        name = [i.get('name') for i in provider]#
        provider_site = [i.get('url') for i in provider]#
  
        clickThroughUrl = [i.get('clickThroughUrl') for i in content]
        url = ['-' if i is None else (i.get('url') or '-') for i in (clickThroughUrl or [])]



        id = [i.get('id') for i in content]

        originalUrl = [i.get('originalUrl') for i in thumbnail]#
  

        dict = { 
            'id': id,
            'title': title,
            'description': description,
            'summary': summary,
            'url': url,
            'date': pubDate,
            'time': displayTime,
            'provider_name': name,
            'provider_site': provider_site,
            'original_url': originalUrl,
        }

        return pd.DataFrame(dict)
        

    async def recommendations(self, ticker):

        data = yf.Ticker(ticker)
        data = data.recommendations
        data['ticker'] = ticker
        return data


    async def revenue_estimate(self, ticker):

        data = yf.Ticker(ticker)
        data = data.revenue_estimate
        data.columns = [camel_to_snake(col) for col in data.columns]

        data['ticker'] = ticker
        data.reset_index(inplace=True)
        return data
    

    async def sec_filings(self, ticker):

        data = yf.Ticker(ticker)
        data = data.sec_filings

        date = [i.get('date') for i in data]
        type = [i.get('type') for i in data]
        edgar_url = [i.get('edgarUrl') for i in data]
        exhibits = [i.get('exhibits') for i in data]
        # Prepare a list of rows for the final DataFrame.
        rows = []
        for filing in data:
            filing_date = filing.get('date')
            filing_type = filing.get('type')
            filing_edgar_url = filing.get('edgarUrl')
            exhibits = filing.get('exhibits')
            
            if exhibits is None:
                # If there are no exhibits for this filing, add one row with a placeholder.
                rows.append({
                    'date': filing_date,
                    'type': filing_type,
                    'edgar_url': filing_edgar_url,
                    'exhibit': None,
                    'exhibit_url': '-'  # or any placeholder value you prefer
                })
            else:
                # For each exhibit in the exhibits dict, create a separate row.
                for exhibit_key, exhibit_url in exhibits.items():
                    rows.append({
                        'date': filing_date,
                        'type': filing_type,
                        'edgar_url': filing_edgar_url,
                        'exhibit': exhibit_key,
                        'exhibit_url': exhibit_url
                    })

        # Now create the DataFrame.
        df = pd.DataFrame(rows)

        return df
    



    # async def sustainability(self, ticker):
    #     import pandas as pd
    #     # Normalize ticker
    #     ticker = ticker.upper().strip()
        
    #     # Get sustainability data from yfinance
    #     t = yf.Ticker(ticker)
    #     data = t.sustainability  # Expected to be a dict of ESG metrics
        
    #     # If no sustainability data is available, return a placeholder DataFrame
    #     if data is None:
    #         return pd.DataFrame({'ticker': [ticker], 'error': ['No sustainability data available']})
        
    #     # Add the ticker to the data dictionary
    #     data['ticker'] = ticker
        
    #     # Create a single-row DataFrame from the dictionary.
    #     df = pd.DataFrame([data])
    #     return df


# # For production testing and demonstration of database usage
# if __name__ == '__main__':
#     import asyncio

#     sdk = YfSDK()
#     test_ticker = "AAPL"  # Change this ticker symbol for testing

#     async def run_tests():
#         logger.info("Fetching balance sheet for %s", test_ticker)
#         bs = await sdk.balance_sheet(test_ticker)
#         logger.info("Balance Sheet (first 5 rows):\n%s", bs.head())

#         logger.info("Fetching cash flow for %s", test_ticker)
#         cf = await sdk.get_cash_flow(test_ticker)
#         logger.info("Cash Flow (first 5 rows):\n%s", cf.head())

#         logger.info("Fetching all candles for %s", test_ticker)
#         candles = await sdk.get_all_candles(test_ticker)
#         logger.info("Candles (first 5 rows):\n%s", candles.head())

#         logger.info("Fetching dividends for %s", test_ticker)
#         div = sdk.dividends(test_ticker)
#         logger.info("Dividends:\n%s", div)

#         logger.info("Fetching fast info for %s", test_ticker)
#         fi = sdk.fast_info(test_ticker)
#         logger.info("Fast Info:\n%s", fi)

#         logger.info("Fetching financials for %s", test_ticker)
#         fin = sdk.financials(test_ticker)
#         logger.info("Financials:\n%s", fin)

#         logger.info("Fetching income statement for %s", test_ticker)
#         inc = await sdk.income_statement(test_ticker)
#         logger.info("Income Statement (first 5 rows):\n%s", inc.head())

#         logger.info("Fetching info for %s", test_ticker)
#         info = await sdk.get_info(test_ticker)
#         logger.info("Info (first 5 rows):\n%s", info.head())

#         logger.info("Fetching institutional holdings for %s", test_ticker)
#         inst = await sdk.institutional_holdings(test_ticker)
#         logger.info("Institutional Holdings (first 5 rows):\n%s", inst.head())

#         logger.info("Fetching mutual fund holders for %s", test_ticker)
#         mf = await sdk.mutual_fund_holders(test_ticker)
#         logger.info("Mutual Fund Holders (first 5 rows):\n%s", mf.head())

#         logger.info("Fetching ATM calls for %s", test_ticker)
#         calls = await sdk.atm_calls(test_ticker)
#         logger.info("ATM Calls (first 5 rows):\n%s", calls.head())

#         logger.info("Fetching ATM puts for %s", test_ticker)
#         puts = await sdk.atm_puts(test_ticker)
#         logger.info("ATM Puts (first 5 rows):\n%s", puts.head())

#     asyncio.run(run_tests())
