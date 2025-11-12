import httpx
import os
from dotenv import load_dotenv
load_dotenv()
from .whale_models import MarketTide, AllTide, ETFFlow, TopNetPremium, TopSectorPremium, DarkPools, TickerAggregates, AnalystResults, MarketState, CompanyData, HistoricChains, DailyOptionBars, OptionSummary
from datetime import datetime , timedelta
today = datetime.now().strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')



class WhaleSDK:
    def __init__(self):
        self.key = os.environ.get('WHALES_KEY')
        self.headers = { 
    'Content-Type': 'application/json',
    'Authorization': f"Bearer {self.key}",
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
        }




    async def market_tide(self, date:str=yesterday, grouping_minutes:str='1'):
        """Get market tide by date and grouping minutes.
        
        DATE: yyyy-mm-dd
        """

        endpoint = f"https://phx.unusualwhales.com/api/net-flow-ticks?date={date}&grouping_minutes={grouping_minutes}&market_day_timeframe=1"

        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint)
            data = data.json()

            data = data['data']

            return MarketTide(data)



    async def all_tide(self, date:str='2024-03-26', timeframe:str='1min'): 
        """Get market tide by date and timeframe.
        DATE: yyyy-mm-dd

        >>> TIMEFRAMES:

            1min
            5min
            10min

        """

        endpoint = f"https://phx.unusualwhales.com/api//net_flow/second?date={date}&time_frame={timeframe}"

        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint)
            data = data.json()

            data = data['data']

            return AllTide(data)




    async def etf_flow(self):
        """Get ETFs flow"""

        endpoint= f"https://phx.unusualwhales.com/api/sector/etfs"

        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint)
            data = data.json()

            data = data['data']

            return ETFFlow(data)



    async def top_net_premiums(self):
        """Top net premium options."""

        endpoint = f"https://phx.unusualwhales.com/api/top_net_premiums?"



        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint)
            data = data.json()

            data = data['data']
            return TopNetPremium(data)
     




    async def top_sector_premium(self, sector:str='Technology'):
        """Top premium by sector.
        
        SECTORS:

        >>> Technology
        >>> Industrials
        >>> Energy
        >>> Utilities
        >>> Health Care
        >>> Consumer Staples
        >>> Consumer Disc
        >>> Communication
        >>> Financials
        >>> Materials
        >>> Real Estate
        
        """


        endpoint = f"https://phx.unusualwhales.com/api/top_net_premiums?sectors[]={sector}"


        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint)
            data = data.json()

            data = data['data']
            return TopSectorPremium(data)



    async def dark_pools(self, limit:str='50'):
        """Get dark pool data for stocks."""
        endpoint = f"https://phx.unusualwhales.com/api/flow/dark-pool?limit={limit}"


        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint)
            data = data.json()

            data = data['trades']

            return DarkPools(data)


    async def ticker_aggregates(self, ticker:str='AAPL'):
        """Get options aggregates by the ticker."""


        endpoint = f"https://phx.unusualwhales.com/api/ticker_aggregates/{ticker}"



        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint)
            data = data.json()


            return TickerAggregates(data)
        

    async def analyst_results(self, ticker:str='AAPL'):
        """Get detailed analyst ratings for a ticker."""

        endpoint = f"https://phx.unusualwhales.com/api/analyst_results/?ticker={ticker}"



        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint)
            data = data.json()
            data = data['data']

            return AnalystResults(data)


    async def market_state(self, ticker:str='AAPL', limit:str='1'):
        """Get options market state for a ticker."""

        endpoint = f"https://phx.unusualwhales.com/api/market_state_all/{ticker}?limit={limit}"



        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint)
            data = data.json()

            return MarketState(data)



    async def company_data(self, ticker:str='AAPL'):
        """Get detailed company data metrics for a ticker."""

        endpoint = f"https://phx.unusualwhales.com/api/companies/{ticker}?thin=true"



        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint)
            data = data.json()
            data = data['company']
            return CompanyData(data)

    async def historic_chains(self, option_symbol:str='CMCSA240419P00042500', date:str=today):
        """Get historic chain data for an option contract."""

        endpoint = f"https://phx.unusualwhales.com/api/historic_chains/{option_symbol}?date={date}"



        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint)
            data = data.json()
            data = data['chains']
            return HistoricChains(data)

    async def daily_option_bars(self, option_symbol:str='CMCSA240419P00042500'):
        endpoint = f"https://phx.unusualwhales.com/api/chain_aggregates/{option_symbol}/daily_bars"

        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint)
            data = data.json()
            data = data['data']
            return DailyOptionBars(data)









    async def option_summary(self, ticker:str='AAPL', timespan:str='1m'):
        """Get detailed option summary for a ticker."""


        endpoint = f"https://phx.unusualwhales.com/api/osummaries/{ticker}?timespan={timespan}"

        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint)
            data = data.json()
            data = data['data']
            return OptionSummary(data)