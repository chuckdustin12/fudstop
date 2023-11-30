import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
from dotenv import load_dotenv
load_dotenv()
from typing import Optional
import os
import sys
import pandas as pd
from pathlib import Path
import requests
from asyncio import Lock
from datetime import datetime, timedelta
from ..webull.webull_trading import WebullTrading
import numpy as np
from urllib.parse import unquote
lock = Lock()
trading = WebullTrading()
# Function to map Pandas dtypes to PostgreSQL types
def dtype_to_postgres(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'REAL'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    elif pd.api.types.is_string_dtype(dtype):
        return 'TEXT'
    else:
        return 'TEXT'  # Default type
class PolygonSync:
    def __init__(self):
        self.connection_string = os.environ.get('POLYGON_STRING')

        self.database = os.environ.get('DB_NAME')

        self.api_key = os.environ.get('YOUR_POLYGON_KEY')
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        self.one_year_from_now = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
        self.one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')



    def get_polygon_logo(self, symbol: str) -> Optional[str]:
            """
            Fetches the URL of the logo for the given stock symbol from Polygon.io.

            Args:
                symbol: A string representing the stock symbol to fetch the logo for.

            Returns:
                A string representing the URL of the logo for the given stock symbol, or None if no logo is found.

            Usage:
                To fetch the URL of the logo for a given stock symbol, you can call:
                ```
                symbol = "AAPL"
                logo_url = await sdk.get_polygon_logo(symbol)
                if logo_url is not None:
                    print(f"Logo URL: {logo_url}")
                else:
                    print(f"No logo found for symbol {symbol}")
                ```
            """
            url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={self.api_key}"
            data = requests.get(url).json()
                    
            if 'results' not in data:
                # No results found
                return None
            
            results = data['results']
            branding = results.get('branding')

            if branding and 'icon_url' in branding:
                encoded_url = branding['icon_url']
                decoded_url = unquote(encoded_url)
                url_with_api_key = f"{decoded_url}?apiKey={self.api_key}"
                return url_with_api_key