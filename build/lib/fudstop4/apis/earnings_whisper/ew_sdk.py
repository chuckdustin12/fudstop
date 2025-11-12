import requests
import os
import httpx
from dotenv import load_dotenv
load_dotenv()
session = requests.session()

from .ew_models import TopSentimentHeatmap, SpyData, UpcomingRussellAndSectors, DatedChartData, Messages, Pivots, TodaysResults, CalData, ChartData
from datetime import datetime

# Get the current date
current_date = datetime.now()

# Format the date as "yyyymmdd"

class EarningsWhisper:
    def __init__(self):
        self.headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9",
            "Dnt": "1",
            "Origin": "https://www.earningswhispers.com",
            "Referer": "https://www.earningswhispers.com/",
            "Sec-Ch-Ua": "\"Google Chrome\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"Windows\"",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "X-Client-Data": f"CJa2yQEIo7bJAQipncoBCNWTywEIkqHLAQiFoM0BCLeFzgEIpqLOAQiPqM4BGPbJzQEY642lFw=="
        }
        self.base_url = "https://www.earningswhispers.com/api"
        self.today = current_date.strftime("%Y%m%d")

    def fetch(self, endpoint):
        response = session.get(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            print(f'Error - couldnt retrieve data for {endpoint}')


    async def get_chart_data(self, ticker:str):
        '''Pulls chart data from EW
        
        Arguments:

        >>> ticker: the ticker to query data for
        
        '''
        endpoint = f"/getchartdata/{ticker}"

        data = self.fetch(self.base_url+endpoint)
        for i in data[0]:
            print(f"self.{i} = [i.get('{i}') for i in data]")
        return ChartData(data)
    

    async def get_top_sentiment(self):
        """
        Returns top tickers by sentiment towards earnings.
        
        """
        try:
            url ="https://www.earningswhispers.com/api/gettopsentheat"
            async with httpx.AsyncClient(headers=self.headers) as client:
                data = await client.get(url)
                data = data.json()
                data = TopSentimentHeatmap(data)
                return data
        except Exception as e:
            print(e)
    

    async def get_spy_data(self):
        try:
            url = "https://www.earningswhispers.com/api/getspydata"
            async with httpx.AsyncClient(headers=self.headers) as client:
                data = await client.get(url)
                data = data.json()
                data = SpyData(data)
                return data
        except Exception as e:
            print(e)
    

    async def upcoming_russell(self):
        try:
            url="https://www.earningswhispers.com/api/upcomingrussell"

            async with httpx.AsyncClient(headers=self.headers) as client:
                data = await client.get(url)
                data = data.json()
                data = UpcomingRussellAndSectors(data)
                return data
        except Exception as e:
            print(e)


    async def upcoming_sectors(self):
        try:
            url="https://www.earningswhispers.com/api/upcomingsectors"
            async with httpx.AsyncClient(headers=self.headers) as client:
                data = await client.get(url)
                data = data.json()
                data = UpcomingRussellAndSectors(data)
                return data
        except Exception as e:
            print(e)
    

    async def dated_chart_data(self, ticker, date:str=None):
        """
        Date format: yyyymmdd
        """
        try:
            if date is None:
                date = self.today
            url=f"https://www.earningswhispers.com/api/getdatedchartdata?s={ticker}&d={date}"
            async with httpx.AsyncClient(headers=self.headers) as client:
                data = await client.get(url)
                data = data.json()
                data = DatedChartData(data)
                return data
        except Exception as e:
            print(e)



    def messages(self):
        r = requests.get("https://www.earningswhispers.com/api/wrs",headers=self.headers).json()
        data = Messages(r)
        return data
    

    async def pivot_list(self):
        try:
            url="https://www.earningswhispers.com/api/pivotlist"
            async with httpx.AsyncClient(headers=self.headers) as client:
                data = await client.get(url)
                data = data.json()
                data = Pivots(data)
                return data
        except Exception as e:
            print(e)
    

    async def todays_results(self):
        try:
            url="https://www.earningswhispers.com/api/todaysresults"
            async with httpx.AsyncClient(headers=self.headers) as client:
                data = await client.get(url)
                data = data.json()
                data = TodaysResults(data)
                return data
        except Exception as e:
            print(e)

    async def calendar(self, date:str=None):
        """
        Date format:

        yyyymmdd
        
        """
        try:
            if date is None:
                date = self.today
            r = f"https://www.earningswhispers.com/api/caldata/{date}"
            async with httpx.AsyncClient(headers=self.headers) as client:
                data = await client.get(r)

                data = data.json()
                data = CalData(r)
                return data
        except Exception as e:
            print(e)

