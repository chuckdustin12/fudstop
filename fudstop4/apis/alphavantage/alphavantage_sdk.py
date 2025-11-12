import httpx
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()
from .models import HistoricOptions, NewsSentiments, InsiderTransactions



class AlphaVantageSDK:
    def __init__(self):

        self.alpha_key = os.environ.get('YOUR_ALPHAVANTAGE_KEY')



    async def historic_options(self, ticker:str):
        
        url = f"https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS&symbol={ticker}&apikey={self.alpha_key}"


        async with httpx.AsyncClient() as client:
            data =  await client.get(url)
            data = data.json()

            data = data['data']


            return HistoricOptions(data)
        


    async def news_sentiments(self, tickers: list[str]):

        url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={tickers}&apikey={self.alpha_key}"

        async with httpx.AsyncClient() as client:
            data = await client.get(url)

            data = data.json()

            feed = data['feed']


            return NewsSentiments(feed)
        

    async def insider_transactions(self, ticker):

        url = f"https://www.alphavantage.co/query?function=INSIDER_TRANSACTIONS&symbol={ticker}&apikey={self.alpha_key}"

        async with httpx.AsyncClient() as client:
            data = await client.get(url)

            data = data.json()
            data = data['data']

            return InsiderTransactions(data)