import pandas as pd
import httpx
import asyncio
import aiohttp
from .models.ortex_models import Screener

class OrtexSDK:
    def __init__(self):
        self.cookie = { }



    async def screener(self):

        async with aiohttp.ClientSession() as session:
            async with session.get("https://ortex-gui.ortex.com/interface/api/universe/7/shorts/screener/list?GUIv=2&page_size=100&page=1&initial=true") as resp:

                data = await resp.json()
                rows = data['rows']
                rows = Screener(rows)

                return rows
        