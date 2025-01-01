import sys
from pathlib import Path
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import os
import aiohttp
import csv
import asyncio
import requests
from discord_webhook import AsyncDiscordWebhook, DiscordEmbed
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from apis.webull.webull_markets import WebullMarkets
from apis.webull.webull_trading import WebullTrading
from apis.polygonio.async_polygon_sdk import Polygon
from apis.polygonio.polygon_options import PolygonOptions
from _markets.list_sets.ticker_lists import most_active_tickers
from discord_.embeddings import Embeddings
from discord_webhook import AsyncDiscordWebhook, DiscordEmbed


class ConditionalScans(PolygonOptions):
    def __init__(self, **kwargs):
        self.color = kwargs.get('color', None)
        self.hook = kwargs.get('hook', None)
        self.velocity_scan = os.environ.get('velocity_scan')


    async def send_webhook(self):

        if self.color is not None and self.hook is not None:

            self.hook = AsyncDiscordWebhook(self.hook)
            await self.hook.execute()

