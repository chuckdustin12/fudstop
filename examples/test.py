import os
from fudstop.scripts.rsi_screener import gather_rsi_results
min1_td9 = os.environ.get("min1_td9")
min5_td9 = os.environ.get("min5_td9")
min15_td9 = os.environ.get("min15_td9")
min20_td9 = os.environ.get("min20_td9")
min30_td9 = os.environ.get("min30_td9")
min60_td9 = os.environ.get("min60_td9")
min120_td9 = os.environ.get("min120_td9")
min240_td9 = os.environ.get("min240_td9")
day_td9 = os.environ.get("day_td9")
min2_td9 = os.environ.get("min2_td9")
min3_td9 = os.environ.get("min3_td9")
min10_td9 = os.environ.get("min10_td9")
week_td9 = os.environ.get("week_td9")
access_token = os.environ.get("ACCESS_TOKEN")
month_td9 = os.environ.get("month_td9")

import asyncio

asyncio.run(gather_rsi_results(timespan='minute', webhook_url=min10_td9))