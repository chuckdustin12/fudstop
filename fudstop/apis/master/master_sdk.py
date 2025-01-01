import sys
from pathlib import Path
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import os
from dotenv import load_dotenv
load_dotenv()
import json
import inspect
from .tools import fudstop_tools
import pandas as pd
from fudstop._markets.list_sets.dicts import healthcare,energy,etfs,real_estate,financial_services,communication_services,consumer_cyclical,consumer_defensive,utilities,industrials,basic_materials,technology
all_tickers = healthcare+energy+etfs+real_estate+financial_services+communication_services+consumer_cyclical+consumer_defensive+utilities+industrials+basic_materials+technology
from fudstop._markets.list_sets.ticker_lists import most_active_tickers
from apis.polygonio.async_polygon_sdk import Polygon
from apis.polygonio.polygon_options import PolygonOptions
from apis.polygonio.polygon_database import PolygonDatabase
from apis.webull.webull_trading import WebullTrading
from apis.webull.webull_markets import WebullMarkets
from apis.occ.occ_sdk import occSDK
from apis.newyork_fed.newyork_fed_sdk import FedNewyork
from apis.fed_print.fedprint_sdk import FedPrint
from apis.earnings_whisper.ew_sdk import EarningsWhisper
from apis.federal_register.fed_register_sdk import FedRegisterSDK
from apis.treasury.treasury_sdk import Treasury
from fudstop.apis.webull.webull_options.webull_options import WebullOptions
from datetime import datetime, timedelta
from openai import OpenAI
import httpx
import asyncio
from datetime import date, datetime
etf_list = pd.read_csv('files/etf_list.csv')
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        # Let the base class default method raise the TypeError
        return super().default(obj)

def custom_json_dumps(data):
    return json.dumps(data, cls=CustomJSONEncoder)

def is_etf(symbol):
    """Check if a symbol is an ETF."""
    return symbol in etf_list['Symbol'].values

class MasterSDK:
    def __init__(self):
        self.trading = WebullTrading()
        self.markets = WebullMarkets(host='localhost', database='market_data', password='fud', port=5432, user='chuck')
        self.occ = occSDK(host='localhost', database='market_data', password='fud', port=5432, user='chuck')
        self.fed = FedNewyork()
        self.fedprint = FedPrint()
        self.client = OpenAI(api_key=os.environ.get('OPENAI_KEY'))
        self.ew = EarningsWhisper() #sync
        self.treas = Treasury(host='localhost', database='fudstop', password='fud', port=5432, user='chuck')
        self.poly = Polygon(host='localhost', database='fudstop', password='fud', port=5432, user='chuck')
        self.poly_opts =PolygonOptions(host='localhost', database='fudstop', password='fud', port=5432, user='chuck')
        self.db= PolygonDatabase(host='localhost', database='fudstop', password='fud', port=5432, user='chuck')
        self.wb_opts = WebullOptions(database='fudstop')
        self.register = FedRegisterSDK()
        self.fed = FedNewyork()
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d') 
           
        self.available_functions = {
            'all_poly_options': self.all_poly_options,
            'all_webull_options': self.all_webull_options,
            'occ_options': self.occ_options,
            'webull_analysts': self.webull_analysts,
            'webull_capital_flow': self.webull_capital_flow,
            'webull_etf_holdings': self.webull_etf_holdings,
            'webull_financials': self.webull_financials,
            'webull_highs_lows': self.webull_highs_lows,
            'webull_institutions': self.webull_institutions,
            'webull_news': self.webull_news,
            'webull_short_interest': self.webull_short_interest,
            'webull_top_active': self.webull_top_active,
            'webull_top_gainers': self.webull_top_gainers,
            'webull_top_losers': self.webull_top_losers,
            'webull_top_options': self.webull_top_options,
            'webull_vol_anal': self.webull_vol_anal,
            'webull_cost_distribution': self.webull_cost_distribution,
            'webull_company_brief': self.webull_company_brief,
            'fed_soma': self.fed_soma,
            'fed_ambs': self.fed_ambs,
            'fed_marketshare': self.fed_marketshare,
            'fed_liquidity_swaps': self.fed_liquidity_swaps,
            'fed_repo': self.fed_repo,
            'fed_treasury': self.fed_treasury,
            'fed_securities_lending': self.fed_securities_lending,
            'occ_stock_info': self.occ_stock_info,
            'occ_trending': self.occ_trending,
            'occ_monitor': self.occ_monitor
        }




    def serialize_record(self,record):
        # Check if the record is a Pandas DataFrame or Series
        if isinstance(record, pd.DataFrame):
            # Convert DataFrame to a dictionary
            return record.to_dict(orient='records')
        elif isinstance(record, pd.Series):
            # Convert Series to a list
            return record.tolist()
        else:
            # For any other type, return it as is or handle serialization differently
            return record
    async def batch_insert_dataset(self, dataset: pd.DataFrame, table_name, unique_columns) -> None:
        """Auto batch inserts the dataframe into postgres SQL database."""

        await self.db.batch_insert_dataframe(dataset, table_name=table_name, unique_columns=unique_columns)



    async def webull_short_interest(self, ticker, limit:int=20, insert:bool=False):
        """Get short interest data for a ticker."""
        data = await self.trading.get_short_interest(ticker)
        df = data.df
        df['ticker'] = ticker
        if insert == True:
            await self.batch_insert_dataset(df, table_name='short_int', unique_columns='ticker')

        return df.head(limit).to_dict('records')
    

    async def webull_vol_anal(self, ticker, limit:int=50, insert:bool=False):
        "Get volume analysis data for a ticker"""
        data = await self.trading.volume_analysis(ticker)
        df = data.df
        df['ticker'] = ticker
        if insert == True:
            await self.batch_insert_dataset(df, table_name='vol_anal', unique_columns='ticker')
        return df.head(limit).to_dict('records')


    async def webull_institutions(self, ticker, limit:int=50, insert:bool=False):
        """Get institutional ownership data for a ticker."""
        if is_etf(ticker):
            return "Ticker is an ETF. Try again."
        data = await self.trading.institutional_holding(ticker)
        df = data.as_dataframe
        df['ticker'] = ticker
        if insert == True:
            await self.batch_insert_dataset(df, table_name='inst_holding', unique_columns='ticker')
        return df.head(limit).to_dict('records')
    

    async def webull_analysts(self, ticker, limit:int=50, insert:bool=False):
        """Get analyst ratings for a ticker"""
        if is_etf(ticker):
            return "Ticker is an ETF. Try again."
        data = await self.trading.get_analyst_ratings(ticker)
        df = data.df
        df['ticker'] = ticker
        if insert == True:
            await self.batch_insert_dataset(df, table_name='analysts', unique_columns='ticker')
        return df.head(limit).to_dict('records')
    
    async def webull_financials(self, ticker, type:str='balancesheet', limit:int=4, insert:bool=False):
        """Get webull financial data for a ticker.
        
        
        Args:

        >>> ticker
        >>> type = the type of financials (balancesheet, cashflow, incomestatement)
        """
        if is_etf(ticker):
            return "Ticker is an ETF. Try again."
        data = await self.trading.financials(symbol=ticker, financials_type=type)

        df = pd.DataFrame(data)
        df['ticker'] = ticker
        if insert == True:
            await self.batch_insert_dataset(df, table_name='financials', unique_columns='ticker')
        
        return df.head(limit).to_dict('records')
        
    async def webull_etf_holdings(self, ticker, limit:int=25, insert:bool=False):
        """Gets ETF holdings for a non-etf symbol."""
        if is_etf(ticker):
            return "Ticker is an ETF. Try again."
        
        data = await self.trading.etf_holdings(ticker)
        df = data.df
        df['ticker'] = ticker
        if insert == True:
            await self.batch_insert_dataset(df, table_name='etf_holdings', unique_columns='ticker')
        return df.head(limit).to_dict('records')

    async def webull_news(self, ticker, limit:int=5, insert:bool=False):
        """Gets the latest news for a ticker."""
        data = await self.trading.news(ticker)
        df = data.df
        df['ticker'] = ticker
        if insert == True:
            await self.batch_insert_dataset(df, table_name='wb_news', unique_columns='ticker')
        return df.head(limit).to_dict('records')
    async def webull_capital_flow(self, ticker, limit:int=3, insert:bool=False):
        """Gets capital flow data broken down by player size for a ticker."""
        data, HISTORIC = await self.trading.capital_flow(ticker)
        df = data.df
        df['ticker'] = ticker
        if insert == True:
            await self.batch_insert_dataset(df, table_name='cap_flow', unique_columns='ticker')
        return df.head(limit).to_dict('records')
    


    async def webull_top_gainers(self, type:str='preMarket', limit:int=20, insert:bool=False):
        """Gets the top 20 gainers on the day by type.
        
        >>> TYPES: 

        preMarket
        afterMarket
        5min
        1d
        5d
        3m
        52w
        """


        data = await self.markets.get_top_gainers(rank_type=type)
        
        data['type'] = type.lower()
        if insert == True:
            await self.batch_insert_dataset(data, table_name=f'top_gainers_{type}', unique_columns='symbol, type')
        return data.head(limit).to_dict('records')
    


    async def webull_top_losers(self, type:str='preMarket', limit:int=20, insert:bool=False):
        """Gets the top 20 losers on the day by type.
        
        >>> TYPES: 

        preMarket
        afterMarket
        5min
        1d
        5d
        3m
        52w
        """


        data = await self.markets.get_top_losers(rank_type=type)
        data['type'] = type.lower()
        if insert == True:
            await self.batch_insert_dataset(data, table_name=f'top_losers_{type}', unique_columns='symbol, type')
        return data.head(limit).to_dict('records')
    

    async def webull_top_options(self, type:str='volume', limit:int=20, insert:bool=False):
        """Gets the top 20 top options on the day by type.
        
        >>> TYPES: 

        totalVolume
        totalPosition
        volume
        position
        impVol
        turnover
        posIncrease
        posDecrease
        """


        data = await self.markets.get_top_options(rank_type=type)
        data['type'] = type.lower()
        if insert == True:
            await self.batch_insert_dataset(data, table_name=f'top_options_{type}', unique_columns='symbol, type')
        return data.head(limit).to_dict('records')
    

    
    async def webull_top_active(self, type:str='rvol10', limit:int=20, insert:bool=False):
        """Gets the top 20 top options on the day by type.
        
        >>> TYPES: 

        rvol10d
        turnover
        range
        """


        data = await self.markets.get_most_active(rank_type=type, as_dataframe=True)
        if insert == True:
            await self.batch_insert_dataset(data, table_name=f'top_active', unique_columns='symbol')
        return data.head(limit).to_dict('records')
    



    # async def webull_earnings(self, date:int=None, limit:int='20'):
    #     """Get earnings for a specific date."""


    #     data = await self.markets.earnings(start_date=date)

    #     await self.batch_insert_dataset(data, table_name=f'earnings', unique_columns='releasedate')
    #     return data.head(limit).to_dict('records')



    async def webull_highs_lows(self, type:int='newHigh', limit:int=20, insert:bool=False):
        """Get tickers pushing 52w highs and lows or near them.
        >>> TYPES:

            newHigh
            newLow
            nearHigh
            nearLow

        
        """
        


        data = await self.markets.highs_and_lows(type)

        if insert == True:
            await self.batch_insert_dataset(data, table_name=f'{type}', unique_columns='symbol')
        return data.head(limit).to_dict('records')
    

    async def webull_cost_distribution(self, ticker:str, start_date=None, end_date=None, limit:int=10, insert:bool=False):
        """Get % of players in profit for a ticker."""

        data = await self.trading.cost_distribution(ticker)
        df = data.df
        df['ticker'] = ticker

        if insert == True:

            await self.batch_insert_dataset(df, table_name='cost_dist', unique_columns='ticker')

        return df.head(limit).to_dict('records')
    

    async def webull_company_brief(self, ticker:str):
        """Get % of players in profit for a ticker."""

        companyBrief_df, executives_df, sectors_df = await self.trading.company_brief(ticker)



        return companyBrief_df.to_dict('records'), executives_df.to_dict('records'), sectors_df.to_dict('records')


    async def all_webull_options(self, ticker:str, limit:int=50, insert:bool=False):
        """Get all options for a ticker."""

        

        base_data, from_, opts = await self.wb_opts.all_options(ticker)
        vol_1y = base_data.vol1y
        opts = opts.as_dataframe
        data = opts
        data['vol1y'] = vol_1y
        data['underlying_price'] = base_data.close
        if insert == True:
            await self.batch_insert_dataset(data, table_name=f'wb_opts', unique_columns='option_symbol')
        return data.head(limit).sort_values('vol').to_dict('records')



    async def all_poly_options(self, ticker:str, limit:int=50, contract_type=None, strike_price_gte=None, strike_price_lte=None, expiry_date_gte=None, expiry_date_lte=None, insert:bool=False):
        """Get all options for a ticker."""

        

        data = await self.poly_opts.get_option_chain_all(ticker)
 
        if insert == True:
            await self.batch_insert_dataset(data.df, table_name=f'poly_opts', unique_columns='option_symbol')
        return data.df.head(limit).sort_values('vol').to_dict('records')


    async def occ_options(self, ticker:str, limit:int=50, insert:bool=False):
        """Get occ options data for a ticker."""
        

        data = await self.occ.options_monitor(ticker)

        data['ticker'] = ticker
        if insert == True:
            await self.batch_insert_dataset(data, table_name=f'occ_opts', unique_columns='ticker')
        return data.head(limit).to_dict('records')
    


    async def fed_ambs(self, limit:int=25, insert:bool=False):
        """Get fed agency mortgage backed security data"""
        try:
            data = self.fed.all_agency_mortgage_backed_securities()

            data = data.rename(columns={'operationDate': 'operation_date'})
            if insert == True:
                await self.batch_insert_dataset(data, table_name='fed_ambs', unique_columns='operation_date')

            return data.head(limit).to_dict('records')
        except Exception as e:
            print(e)
        

    async def fed_liquidity_swaps(self, limit:int=25, insert:bool=False):
        """Get fed central liquidity swap data."""

        data = self.fed.central_bank_liquidity_swaps()
        data = pd.DataFrame(data)
        data = data.rename(columns={'settlementDate': 'settlement_date'})
        if insert == True:
            await self.batch_insert_dataset(data, table_name='fed_swaps', unique_columns='settlement_date')

        return data.head(limit).to_dict('records')
    

    async def fed_securities_lending(self, limit:int=25, insert:bool=False):
        """Get fed securities lending operations."""

        data = self.fed.securities_lending_operations()
        data = data.rename(columns={'operationDate': 'operation_date'})
        if insert == True:
            await self.batch_insert_dataset(data, table_name='fed_lending', unique_columns='operation_date')

        return data.head(limit).to_dict('records')


    async def fed_repo(self, limit:int=25, insert:bool=False):
        """Get fed repo operations data."""

        data = self.fed.reverse_repo()
        data = data.rename(columns={'operationDate': 'operation_date'})
        if insert == True:
            await self.batch_insert_dataset(data, table_name='fed_repo', unique_columns='operation_date')

        return data.head(limit).to_dict('records')
    


    async def fed_treasury(self, limit:int=25, insert:bool=False):
        """Get fed treasury data"""

        data = self.fed.treasury_holdings()
        data = data.rename(columns={'operationDate': 'operation_date'})

        if insert == True:
            await self.batch_insert_dataset(data, table_name='fed_treasury', unique_columns='operation_date')

        return data.head(limit).to_dict('records')
    


    async def fed_marketshare(self, limit:int=25, insert:bool=False):
        """Get market share information from the fed."""

        data = self.fed.market_share()


        if insert == True:
            await self.batch_insert_dataset(data, table_name='fed_marketshare', unique_columns='insertion_timestamp')

        return data.head(limit).to_dict('records')
    

    async def fed_soma(self, limit:int=25, insert:bool=False):
        """Get federal reserve system open market account holdings.."""

        data = self.fed.soma_holdings()
        data = data.rename(columns={'asOfDate': 'operation_date'})
        if insert == True:
            await self.batch_insert_dataset(data, table_name='fed_soma', unique_columns='operation_date')

        return data.head(limit).to_dict('records')
    

    async def fed_research(self, query:str='Monetary Policy', insert:bool=False):
        """Get Documents from the Federal Reserve by search query."""

        data = await self.fedprint.search(filter=query)
        df = data.as_dataframe
        df['query'] = query
        df['keywords'] = [','.join(keywords) if keywords else 'NA' for keywords in df['keywords']]
        if insert == True:
            await self.batch_insert_dataset(df, table_name='fed_research', unique_columns='url')

        return df.head(10).to_dict('records')
    

    async def ew_pivots(self, limit:int=20, insert:bool=False):
        """Get pivot point data for earnings. (support /resistance)"""
        data = await self.ew.pivot_list()

        df = data.as_dataframe
        if insert == True:
            await self.batch_insert_dataset(df, table_name='earnings_pivot', unique_columns='ticker')
        return df.head(limit).to_dict(orient='records')
    

    async def ew_sentiment(self, limit:int=20, insert:bool=False):
        """Get earnings upcoming based on sentiment."""
        data = await self.ew.get_top_sentiment()

        df = data.as_dataframe
        if insert == True:
            await self.batch_insert_dataset(df, table_name='earnings_sentiment', unique_columns='ticker')
        return df.head(limit).to_dict(orient='records')
    
    async def ew_upcoming_sector(self, limit:int=20, insert:bool=False):
        """Get earnings upcoming based on sentiment."""
        data = await self.ew.upcoming_sectors()

        df = data.as_dataframe
        if insert == True:
            await self.batch_insert_dataset(df, table_name='earnings_sector', unique_columns='earnings_date')
        return df.head(limit).to_dict(orient='records')
    

    async def ew_upcoming_russell(self, limit:int=20, insert:bool=False):
        """Get earnings upcoming based on sentiment."""
        data = await self.ew.upcoming_russell()

        df = data.as_dataframe
        if insert == True:
            await self.batch_insert_dataset(df, table_name='earnings_russell', unique_columns='earnings_date')
        return df.head(limit).to_dict(orient='records')
    

    async def ew_today_results(self, limit:int=20, insert:bool=False):
        """Get earnings upcoming based on sentiment."""
        data = await self.ew.todays_results()

        df = data.as_dataframe
        if insert == True:
            await self.batch_insert_dataset(df, table_name='earnings_today_results', unique_columns='ticker')
        return df.head(limit).to_dict(orient='records')
    
    async def ew_calendar(self, limit:int=20, date:str=None, insert:bool=False):
        """Get earnings upcoming based on sentiment."""
        if date == None:
            date = self.thirty_days_from_now
        data = await self.ew.calendar(date=date)

        df = data.as_dataframe
        if insert == True:
            await self.batch_insert_dataset(df, table_name='earnings_calendar', unique_columns='ticker')
        return df.head(limit).to_dict(orient='records')
    
    async def occ_monitor(self, ticker:str, insert:bool=False):
        """Get options data for a symbol from the OCC."""


        data = await self.occ.options_monitor(ticker)
        new_row = pd.DataFrame({'ticker': [ticker]}, index=[0])  # new_index should be the next index in your DataFrame
        data = pd.concat([data, new_row])


        if insert == True:
            await self.batch_insert_dataset(data, table_name='monitor', unique_columns='ticker')
        return data.to_dict('records')
    

    async def occ_trending(self, insert:bool=False):
        """Get earnings upcoming based on sentiment."""


        data = await self.occ.fetch_most_active()

        df = data.as_dataframe
        if insert == True:
            await self.batch_insert_dataset(df, table_name='trending_options', unique_columns='symbol')

        return df.to_dict(orient='records')
    

    async def occ_stock_info(self, ticker:str, insert:bool=False):
        """Get a ton of stock information thats useful."""


        data = await self.occ.stock_info(ticker)

        df = data.as_dataframe
        if insert == True:
            await self.batch_insert_dataset(df, table_name='stock_info', unique_columns='ticker')

        return df.to_dict(orient='records')


    async def check_osob_rsi(self, timespan:str='day', limit:int=20, insert:bool=False):
        """Scans RSI across a timespan
        >>> TIMESPANS:

        minute
        hour
        day
        week
        month
        """
        tasks = [self.poly.rsi(ticker, timespan=timespan) for ticker in all_tickers]
        results = await asyncio.gather(*tasks)

        # Assuming each result in 'results' corresponds to each ticker in 'all_tickers'
        all_data_dicts = []
        for ticker, result in zip(most_active_tickers, results):
            if result is not None and hasattr(result, 'rsi_value') and result.rsi_value is not None and len(result.rsi_value) > 0:
                rsi = result.rsi_value[0]
                status = None  # Initialize 'status'

                if rsi <= 30:
                    status = 'oversold'  # Corrected spelling
                elif rsi >= 70:  # Changed to 'elif' for mutual exclusivity
                    status = 'overbought'

                if status:  # This check is only necessary if 'status' might not be set
                    data_dict = { 
                        'ticker': ticker,
                        'timespan': timespan,
                        'rsi': rsi,
                        'status': status
                    }

                    all_data_dicts.append(data_dict)
        df = pd.DataFrame(all_data_dicts)
        if insert == True:
            await self.batch_insert_dataset(df, table_name='rsi', unique_columns='ticker, timespan, status')
        return df.head(25).to_dict('records')
    

    async def run_conversation(self, query):
        # Step 1: send the conversation and available functions to the model
        messages = [{"role": "user", "content": f"Call the function and go over the results as the options master: {query}"}]
        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=messages,
            tools=fudstop_tools,
            tool_choice="auto",  # auto is default, but we'll be explicit
        )
        response_message = response.choices[0].message
        tool_calls = response_message.tool_calls
        # Step 2: check if the model wanted to call a function
        if tool_calls:
            available_functions = self.available_functions
            # Step 3: call the function
            # Note: the JSON response may not always be valid; be sure to handle errors
    
            messages.append(response_message)  # extend conversation with assistant's reply

            # Step 4: send the info for each function call and function response to the model
            for tool_call in tool_calls:
                function_name = tool_call.function.name
                print(function_name)
                function_to_call = available_functions[function_name]
                function_args = json.loads(tool_call.function.arguments)

                records = await function_to_call(**function_args)

                # Process each record for serialization
                processed_records = [self.serialize_record(record) for record in records]

                # Serialize the list of processed records
                serialized_response = custom_json_dumps(processed_records)

                messages.append({
                    "tool_call_id": tool_call.id,
                    "role": "tool",
                    "name": function_name,
                    "content": serialized_response,
                })

            second_response = self.client.chat.completions.create(
                model="gpt-3.5-turbo-1106",
                messages=messages,
            )  # get a new response from the model where it can see the function response
            return second_response




    async def run_all_ticker_funcs(self, ticker, high_low_type:str='newHigh', top_option_type:str='volume', most_active_type:str='rvol10d', top_gainer_type:str='preMarket', top_loser_type:str='afterMarket'):

        tasks = [ 
            self.all_poly_options(ticker=ticker),
            self.all_webull_options(ticker=ticker),
            self.occ_options(ticker=ticker),
            self.webull_analysts(ticker=ticker),
            self.webull_financials(ticker=ticker),
            #self.webull_earnings(date=self.eight_days_from_now),
            self.webull_capital_flow(ticker=ticker),
            self.webull_etf_holdings(ticker=ticker),
            self.webull_highs_lows(type=high_low_type),
            self.webull_institutions(ticker=ticker),
            self.webull_news(ticker=ticker),
            self.webull_short_interest(ticker=ticker),
            self.webull_top_active(most_active_type),
            self.webull_top_gainers(top_gainer_type),
            self.webull_top_losers(top_loser_type),
            self.webull_top_options(top_option_type),
            self.webull_cost_distribution(ticker=ticker, insert=True),
            self.webull_company_brief(ticker=ticker),
            self.webull_vol_anal(ticker=ticker),
            self.fed_ambs(insert=True),
            self.fed_securities_lending(insert=True),
            self.fed_treasury(insert=True),
            self.fed_liquidity_swaps(insert=True),
            self.fed_repo(insert=True),
            self.fed_liquidity_swaps(insert=True),
            self.fed_soma(insert=True),
            self.fed_research(insert=True),
            self.fed_marketshare(insert=True),
            self.ew_pivots(insert=True),
            self.ew_sentiment(insert=True),
            self.ew_upcoming_russell(insert=True),
            self.ew_upcoming_sector(insert=True),
            self.ew_today_results(insert=True),
            self.ew_calendar(insert=True),
            self.occ_stock_info(ticker=ticker, insert=True),
            self.occ_monitor(ticker=ticker, insert=True),
            self.occ_trending(insert=True),
            
        ]


        await asyncio.gather(*tasks)

    async def run_technical_funcs(self, rsi_timespan:str='day'):
        tasks = [
        self.check_osob_rsi(timespan=rsi_timespan, insert=True)]

        await asyncio.gather(*tasks)

master = MasterSDK()

