import requests
import os
from dotenv import load_dotenv
load_dotenv()
import httpx
import asyncio
import pandas as pd
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
opts = PolygonOptions()

key = os.environ.get('YOUR_FRED_KEY')

from fudstop4.apis.fred.fred_sdk import fredSDK

fred = fredSDK()


async def main():


    x = await fred.release_dates()



    print(x)


asyncio.run(main())