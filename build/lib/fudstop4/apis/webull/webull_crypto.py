from fudstop4.apis.helpers import generate_webull_headers
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
import aiohttp
import asyncio
db = PolygonOptions()
from fudstop4.apis.webull.crypto_models.crypto_data import WebullCryptoData



class WebullCrypto:
    def __init__(self):
        self.headers = generate_webull_headers()


    async def get_crypto_list(self):
        url = f"https://quotes-gw.webullfintech.com/api/bgw/crypto/list"

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url) as resp:
                data = await resp.json()
                data = WebullCryptoData(data)

                return data.as_dataframe