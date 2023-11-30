import requests
import os
from dotenv import load_dotenv
load_dotenv()
session = requests.session()

from .ew_models import TopSentimentHeatmap, SpyData, UpcomingRussellAndSectors, DatedChartData, Messages, Pivots, TodaysResults, CalData
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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "X-Client-Data": f"{os.environ.get('YOUR_EARNINGS_WHISPER_TOKEN')}"
        }
        self.base_url = "https://www.earningswhispers.com/api"
        self.today = current_date.strftime("%Y%m%d")

    def fetch(self, endpoint):
        response = session.get(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            print(f'Error - couldnt retrieve data for {endpoint}')


    def get_chart_data(self, ticker:str):
        '''Pulls chart data from EW
        
        Arguments:

        >>> ticker: the ticker to query data for
        
        '''
        endpoint = f"/getchartdata/{ticker}"

        data = self.fetch(self.base_url+endpoint)
        return data
    

    def get_top_sentiment(self):
        """
        Returns top tickers by sentiment towards earnings.
        
        """
        r = requests.get("https://www.earningswhispers.com/api/gettopsentheat", headers=self.headers).json()
        data = TopSentimentHeatmap(r)
        return data
    

    def get_spy_data(self):
        r = requests.get("https://www.earningswhispers.com/api/getspydata", headers=self.headers).json()
        data = SpyData(r)
        return data
    

    def upcoming_russell(self):
        r = requests.get("https://www.earningswhispers.com/api/upcomingrussell", headers=self.headers).json()
        data = UpcomingRussellAndSectors(r)
        return data


    def upcoming_sectors(self):
        r = requests.get("https://www.earningswhispers.com/api/upcomingsectors", headers=self.headers).json()
        data = UpcomingRussellAndSectors(r)
        return data
    

    def dated_chart_data(self, ticker, date:str=None):
        """
        Date format: yyyymmdd
        """
        if date is None:
            date = self.today
        r = requests.get(f"https://www.earningswhispers.com/api/getdatedchartdata?s={ticker}&d={date}", headers=self.headers).json()
        data = DatedChartData(r)
        return data



    def messages(self):
        r = requests.get("https://www.earningswhispers.com/api/wrs",headers=self.headers).json()
        data = Messages(r)
        return data
    

    def pivot_list(self):
        r = requests.get("https://www.earningswhispers.com/api/pivotlist", headers=self.headers).json()
        data = Pivots(r)
        return data
    

    def todays_results(self):
        r = requests.get("https://www.earningswhispers.com/api/todaysresults", headers=self.headers).json()
        data = TodaysResults(r)
        return data

    def calendar(self, date:str=None):
        """
        Date format:

        yyyymmdd
        
        """
        if date is None:
            date = self.today
        r = requests.get(f"https://www.earningswhispers.com/api/caldata/{date}", headers=self.headers).json()
        data = CalData(r)
        return data

ew = EarningsWhisper()


chart_data = ew.pivot_list()

print(chart_data.as_dataframe.transpose())