import os
from dotenv import load_dotenv
load_dotenv()
import requests
import pandas as pd
import asyncio

from fudstop4.apis.polygonio.async_polygon_sdk import Polygon
from discord_webhook import DiscordWebhook
from datetime import datetime, timedelta, timezone
# Initialize Polygon API and database
poly = Polygon()
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions(database='fudstop3')

# Define RSI thresholds
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
class UltimateSDK:
    def __init__(self):

    # ---------------------------------------------------------------
    # SINGLE-TICKER METHODS (as you already have them)
    # ---------------------------------------------------------------

        self.seen_tickers = set()  # Track already processed tickers
        self.api_key = os.environ.get('YOUR_POLYGON_KEY')
        self.scalar_tickers = ['SPX', 'VIX', 'OSTK', 'XSP', 'NDX', 'MXEF']
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.semaphore = asyncio.Semaphore(40)
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        self.timeframes = ['m1','m5', 'm10', 'm15', 'm20', 'm30', 'm60', 'm120', 'm240', 'd1']
        self.now_timestamp_int = int(datetime.now(timezone.utc).timestamp())
        self.day = int(86400)
        self.ticker_df = pd.read_csv('files/ticker_csv.csv')
        self.id = 15765933
        self.ticker_to_id_map = dict(zip(self.ticker_df['ticker'], self.ticker_df['id']))
        self.wb_headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "access_token": "dc_us_tech1.193f1ba4ca7-5bd586952af0445ea4e4883003c577b1",
    "app": "global",
    "app-group": "broker",
    "appid": "wb_web_app",
    "device-type": "Web",
    "did": "w35fbki4nv4n4i6fjbgjca63niqpo_22",
    "hl": "en",
    "origin": "https://app.webull.com",
    "os": "web",
    "osv": "i9zh",
    "platform": "web",
    "priority": "u=1, i",
    "referer": "https://app.webull.com/",
    "reqid": "h15qdhcy99l2sidi00t2sox8y748h_35",
    "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "tz": "America/Chicago",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "ver": "5.2.1",
    "x-s": "9ad7d1393ca5c705bfe6469622b6805384a53b27c86fcaf5966061737c602747",
    "x-sv": "xodp2vg9"
}
            
    async def upside_downside(self):
        await db.connect()
        

        query = f"""SELECT
        macd.ticker,
        macd.timespan,
        CASE
            WHEN rsi.status = 'overbought' AND macd.sentiment = 'bearish' THEN 'ob_bear'
            WHEN rsi.status = 'oversold' AND macd.sentiment = 'bullish' THEN 'os_bull'
        END AS type
    FROM
        macd
    JOIN
        rsi
    ON
        macd.ticker = rsi.ticker
        AND macd.timespan = rsi.timespan
    WHERE
        ((rsi.status = 'overbought' AND macd.sentiment = 'bearish')
        OR
        (rsi.status = 'oversold' AND macd.sentiment = 'bullish'))
        AND
        macd.insertion_timestamp > NOW() - INTERVAL '2 minutes'
        AND
        rsi.insertion_timestamp > NOW() - INTERVAL '2 minutes';
    """


        results = await db.fetch(query)

        df = pd.DataFrame(results, columns=['ticker', 'timespan', 'type'])
        
    #
        await db.batch_upsert_dataframe(df, table_name='upside_downside', unique_columns=['ticker', 'timespan' ,'type'])

        return df