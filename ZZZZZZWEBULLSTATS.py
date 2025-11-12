import asyncio
from dotenv import load_dotenv
from polars import datetime
load_dotenv()
from helpers import generate_webull_headers
import aiohttp
from datetime import datetime, timezone, timedelta
from fudstop4.apis.webull.webull_trading import WebullTrading
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
opts = PolygonOptions()
from fudstop4._markets.list_sets.ticker_lists import most_active_tickers
import pandas as pd
trading = WebullTrading()

class CallPutProfiles:
    def __init__(self, ticker, callPutProfiles):

        self.ticker = ticker
        self.strikes = [i.get('strikes') for i in callPutProfiles]
        self.calls = [i.get('calls') for i in callPutProfiles]
        self.calls_ratio = [i.get('callsRatio') for i in callPutProfiles]
        self.puts = [i.get('puts') for i in callPutProfiles]
        self.puts_ratio = [i.get('putsRatio') for i in callPutProfiles]
        self.total_volume = [i.get('totalVolume') for i in callPutProfiles]


        self.data_dict = { 
            'strike': self.strikes,
            'calls': self.calls,
            'calls_ratio': self.calls_ratio,
            'puts': self.puts,
            'puts_ratio': self.puts_ratio,
            'total_volume': self.total_volume,
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)
        self.as_dataframe['ticker'] = self.ticker

async def main(ticker):

    ticker_id = trading.ticker_to_id_map.get(ticker)

    url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryCallPutRatio?supportBroker=8&tickerId={ticker_id}&count=1000"



    async with aiohttp.ClientSession(headers=generate_webull_headers()) as session:

        async with session.get(url) as resp:

            data = await resp.json()
            tickerId = data.get('tickerId')

            dates = data.get('dates')
            lastTimestamp = data.get('lastTimestamp')
            lastTimestamp = datetime.fromtimestamp(lastTimestamp / 1000, tz=timezone.utc)

            # Eastern Time (UTC-4 or UTC-5 depending on daylight savings; let's assume current EDT UTC-4)
            eastern_offset = timedelta(hours=-4)
            lastTimestamp = lastTimestamp + eastern_offset

            # Format as YYYY-MM-DD HH:MM:SS
            lastTimestamp = lastTimestamp.strftime("%Y-%m-%d %H:%M:%S")
            callPutFlow = data.get('callPutFlow')
            callPutProfiles = data.get('callPutProfiles')


            totalVolume = callPutFlow.get('totalVolume', None)
            totalOpenInterest = callPutFlow.get('totalOpenInterest', None)
            callAsk = callPutFlow.get('callAsk', None)
            putAsk = callPutFlow.get('putAsk', None)
            callBid = callPutFlow.get('callBid', None)
            putBid = callPutFlow.get('putBid', None)
            callNeutral = callPutFlow.get('callNeutral', None)
            putNeutral = callPutFlow.get('putNeutral', None)
            callTotalVolume = callPutFlow.get('callTotalVolume', None)
            putTotalVolume = callPutFlow.get('putTotalVolume', None)
            callAskRatio = callPutFlow.get('callAskRatio', None)
            putAskRatio = callPutFlow.get('putAskRatio', None)
            callBidRatio = callPutFlow.get('callBidRatio', None)
            putBidRatio = callPutFlow.get('putBidRatio', None)
            callNeutralRatio = callPutFlow.get('callNeutralRatio', None)
            putNeutralRatio = callPutFlow.get('putNeutralRatio', None)

            cp_profiles = CallPutProfiles(ticker=ticker, callPutProfiles=callPutProfiles).as_dataframe

            cp_profiles['total_oi'] = totalOpenInterest

            cp_profiles['last_timestamp'] = lastTimestamp
            cp_profiles['call_ask'] = callAsk
            cp_profiles['put_ask'] = putAsk
            cp_profiles['call_bid'] = callBid
            cp_profiles['put_bid'] = putBid
            cp_profiles['call_neutral'] = callNeutral
            cp_profiles['put_neutral'] = putNeutral
            cp_profiles['call_total_volume'] = callTotalVolume
            cp_profiles['put_total_volume'] = putTotalVolume
            cp_profiles['call_ask_ratio'] = callAskRatio
            cp_profiles['put_ask_ratio'] = putAskRatio
            cp_profiles['call_bid_ratio'] = callBidRatio
            cp_profiles['put_bid_ratio'] = putBidRatio
            cp_profiles['call_neutral_ratio'] = callNeutralRatio
            cp_profiles['put_neutral_ratio'] = putNeutralRatio
            cp_profiles['total_volume'] = totalVolume
            cp_profiles['ticker'] = ticker

            await opts.batch_upsert_dataframe(cp_profiles, 'cp_profiles', unique_columns=['ticker', 'strike'])
           

async def run_main():
    await opts.connect()
    while True:
        # Batch the tickers into chunks of 5
        for i in range(0, len(most_active_tickers), 5):
            batch = most_active_tickers[i:i+5]

            # Run up to 5 concurrently
            tasks = [asyncio.create_task(main(ticker=t)) for t in batch]
            await asyncio.gather(*tasks)

        # optional: small sleep between cycles
        await asyncio.sleep(5)  # prevent hammering the API or CPU



asyncio.run(run_main())