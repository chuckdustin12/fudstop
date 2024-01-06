import sys
from pathlib import Path
import os
import asyncio
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)


from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

from apis.webull.webull_markets import WebullMarkets
from apis.webull.webull_trading import WebullTrading
from list_sets.ticker_lists import most_active_tickers

from discord_embeds.embeddings import Embeddings
from discord_webhook import AsyncDiscordWebhook, DiscordEmbed

trading = WebullTrading()
markets = WebullMarkets()


class ReferenceMarket(Embeddings):
    def __init__(self):
        
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')     
   
        #conditions

        self.accumulation = os.environ.get('accumulation')
        self.fire_sale = os.environ.get('fire_sale')
        self.neutral_zone = os.environ.get('neutral_zone')








    async def volume_analysis_screener(self, ticker:str):

        """
        Scan for fire sale conditions - where the overall  volume on the day is 70% or more sell volume.


        CONDITIONS:


        >>> fire_sale: volume on the day is recorded as 70% or more sell volume.

        >>> accumulation: volume on the day is recorded as 70% or more buy volume.


        >>> neutral_zone: volume on the day is recorded as 70% or more neutral volume.
        
        
        """

        volume_analysis = await trading.volume_analysis(ticker)
        data_dict = getattr(volume_analysis, 'data_dict', None)
        print(ticker)
        if volume_analysis is not None:
            if data_dict is not None:
                data_dict.update({'ticker': ticker})
            if volume_analysis.buyPct >= 70:
                condition = 'accumulation'

                await self.volume_analysis_embed(condition, self.accumulation, data_dict)
                

            
            if volume_analysis.sellPct >= 70:
                condition = 'fire_sale'
                await self.volume_analysis_embed(condition, self.fire_sale, data_dict)


            if volume_analysis.nPct >= 70:
                condition = 'neutral_zone'
                await self.volume_analysis_embed(condition,self.neutral_zone, data_dict)




async def run_reference_market():

    market = ReferenceMarket()

    tasks = [market.volume_analysis_screener(i) for i in most_active_tickers]



    await asyncio.gather(*tasks)



asyncio.run(run_reference_market())