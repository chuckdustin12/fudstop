import os
from dotenv import load_dotenv
load_dotenv()

from fudstop.apis.webull.webull_options import WebullOptions


wb = WebullOptions(os.environ.get('WEBULL_OPTIONS'))

import asyncio
import aiohttp
import pandas as pd
async def main():
    await wb.connect()

asyncio.run(main())