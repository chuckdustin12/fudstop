


import yfinance as yf
from _markets.list_sets.ticker_lists import all_tickers
from datetime import datetime
from apis.polygonio.polygon_database import PolygonDatabase
from ..helpers import lowercase_columns
import pandas as pd
from ..helpers import format_large_numbers_in_dataframe
import numpy as np
# Set options to display all rows and columns
pd.set_option('display.max_rows', None)  # None means show all rows


class yfSDK:
    def __init__(self):
        self.tickers = all_tickers
        self.db = PolygonDatabase(database='market_data', host='localhost', user='chuck', password='fud', port=5432)
   
    async def balance_sheet(self, ticker:str, frequency:str='quarterly', pretty:bool=None, as_dict:bool=False):
        """
        Gets balance sheet information for a ticker.

        Arguments:

        >>> Frequency: The frequency. quarterly / annual (default quarterly)

        >>> As Dict: bool - return as a dictionary (optional - default FALSE)

        >>> Pretty: (optional - pretty prent)

        """

        

        data = yf.Ticker(ticker)
        if pretty == None:
            pretty = False

        balance_sheet = data.get_balance_sheet(freq=frequency,pretty=pretty, as_dict=as_dict)
        await self.db.connect()
        data = balance_sheet.transpose()
        data = lowercase_columns(data)
        data['ticker'] = ticker

        await self.db.batch_insert_dataframe(data, table_name='balance_sheet', unique_columns='ticker')
        formatted_data = format_large_numbers_in_dataframe(balance_sheet)
        return formatted_data 

    

    async def get_cash_flow(self, ticker:str, frequency:str='quarterly', pretty:bool=False, as_dict:bool=False):
        """
        Gets cash flow information for a ticker.

        Arguments:

        >>> Frequency: The frequency. quarterly / annual (default quarterly)

        >>> As Dict: bool - return as a dictionary (optional - default FALSE)

        >>> Pretty: (optional - pretty prent)
        """
        data = yf.Ticker(ticker).get_cash_flow(freq=frequency,pretty=pretty, as_dict=as_dict)
        print(data.columns)
        
        await self.db.connect()
        data = data.transpose()
        data = lowercase_columns(data)
        data['ticker'] = ticker

        await self.db.batch_insert_dataframe(data, table_name='cash_flow', unique_columns='ticker')
        formatted_data = format_large_numbers_in_dataframe(data)


        return formatted_data





    async def get_all_candles(self, tickers:str):
        """
        Gets OHLC, adj.Close and Volume data for ALL DATES

        Arguments:


        >>> Tickers: a list of comma separated tickers. (default ALL TICKERS)

        >>> Period: the period to gather data for. OPTIONS = 
        
            >>> 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo 
            
            
            Intraday data cannot extend last 60 days
        
        """


        try:
            chart_data = yf.download(tickers)
        except Exception as e:
            print(f'Error processing data. - {e}')



            chart_data = pd.DataFrame(chart_data).reset_index(None)

            await self.db.connect()

            data = lowercase_columns(chart_data)
            data['ticker'] = tickers
            return chart_data
        
    

    def dividends(self, ticker:str):
        """
        Returns historic dividends for a ticker
        
        """
        try:
            data = yf.Ticker(ticker).get_dividends()

            return data
        except Exception as e:
            return(f"No dividends found for {ticker}. {e}")
        

    def fast_info(self, ticker:str):
        """
        Arguments:

        >>> Limit: the number of results to return (optional - default 15)
        """
        data = yf.Ticker(ticker=ticker).get_fast_info().items()


        


        df = pd.DataFrame(data)
        df.reset_index(drop=True, inplace=True)

        formatted_data = format_large_numbers_in_dataframe(df)
        return formatted_data 



    def financials(self, ticker:str, frequency:str='quarterly', as_dict:bool=False, pretty:bool=False):

        """
        Gets all financials for a ticker.


        Arguments:

        >>> Frequency: The frequency. quarterly / annual (default quarterly)

        >>> As Dict: bool - return as a dictionary (optional - default FALSE)

        >>> Pretty: (optional - pretty prent)
        """
        data = yf.Ticker(ticker=ticker).get_financials(freq=frequency,as_dict=as_dict, pretty=pretty)

        formatted_data = format_large_numbers_in_dataframe(data)
        return formatted_data 
    

    async def income_statement(self, ticker:str, frequency:str='quarterly', as_dict:bool=False, pretty:bool=False):
        """
        Gets the income statement for a ticker.

        Arguments:

        >>> Frequency: The frequency. quarterly / annual (default quarterly)

        >>> As Dict: bool - return as a dictionary (optional - default FALSE)

        >>> Pretty: (optional - pretty prent)

        """
        data = yf.Ticker(ticker=ticker).get_income_stmt(freq=frequency,as_dict=as_dict,pretty=pretty)

        await self.db.connect()
        data = data.transpose()
        data = lowercase_columns(data)
        data['ticker'] = ticker

        await self.db.batch_insert_dataframe(data, table_name='income_statement', unique_columns='ticker')
        formatted_data = format_large_numbers_in_dataframe(data)
        return formatted_data     
    

    async def get_info(self, ticker:str):
        """
        Returns a large dictionary of information for a ticker.

        Arguments:

        None
        
        """
        data  = yf.Ticker(ticker).get_info()

        df = pd.DataFrame(data)
        await self.db.connect()

        df = lowercase_columns(df)
        df['ticker'] = ticker
        df = df.drop(columns=['companyofficers'])
        await self.db.batch_insert_dataframe(df, table_name='info', unique_columns='ticker')
        formatted_data = format_large_numbers_in_dataframe(df)
        return formatted_data     

    


    async def institutional_holdings(self, ticker:str):
        """
        Gets institutional holdoings.

        Arguments:


        
        """

        data =yf.Ticker(ticker).get_institutional_holders()

 
        await self.db.connect()

        data = lowercase_columns(data)
        data['ticker'] = ticker

        await self.db.batch_insert_dataframe(data, table_name='institutions', unique_columns='ticker')
        formatted_data = format_large_numbers_in_dataframe(data)

        # Convert the '% Out' column to float (if it's not already)
        formatted_data['% out'] = formatted_data['% out'].astype(float)

        # Round the '% Out' column to 3 decimal places
        formatted_data['% out'] = formatted_data['% out'].round(3)
        formatted_data.set_index('date reported', inplace=True)

        return formatted_data


    async def mutual_fund_holders(self, ticker:str):
        """
        Gets mutual fund holders


        Arguments:

        >>> 
        
        """

        data = yf.Ticker(ticker=ticker).get_mutualfund_holders()

        await self.db.connect()

        data = lowercase_columns(data)
        data['ticker'] = ticker

        await self.db.batch_insert_dataframe(data, table_name='mf_holders', unique_columns='ticker')
        formatted_data = format_large_numbers_in_dataframe(data)
        # Convert the '% Out' column to float (if it's not already)
        formatted_data['% out'] = formatted_data['% out'].astype(float)

        # Round the '% Out' column to 3 decimal places
        formatted_data['% out'] = formatted_data['% out'].round(3)
        formatted_data.set_index('date reported', inplace=True)

        return formatted_data     
    

    

    async  def atm_calls(self, ticker:str):
        """
        Gets at the money calls for a ticker.
 
        
        """

        calls = yf.Ticker(ticker)._download_options()

        call_options = calls['calls']


        df = pd.DataFrame(call_options)

        await self.db.connect()

        df = lowercase_columns(df)
        df['ticker'] = ticker
        df['inthemoney'] = df['inthemoney'].astype('boolean')
        df = df.rename(columns={'contractsymbol': "option_symbol"})
        await self.db.batch_insert_dataframe(df, table_name='atm_calls', unique_columns='option_symbol')
        



        return df

    async def atm_puts(self, ticker:str):
        """
        Gets At The Money puts for a ticker.
        
        """

        puts = yf.Ticker(ticker)._download_options()

        put_options = puts['puts']


        df = pd.DataFrame(put_options)


        await self.db.connect()

        df = lowercase_columns(df)
        df['ticker'] = ticker
        df['inthemoney'] = df['inthemoney'].astype('boolean')
        df = df.rename(columns={'contractsymbol': "option_symbol"})

        await self.db.batch_insert_dataframe(df, table_name='atm_puts', unique_columns='option_symbol')



        return df

