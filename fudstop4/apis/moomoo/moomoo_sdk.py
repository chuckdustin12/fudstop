import pandas as pd
import aiohttp
from .moomoo_models import EarningsData



class MooMooSDK:
    def __init__(self):
        self.quote_key= "36bf2e8388"
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36', 'Quote-Token': "36bf2e8388"}


    async def earnings_data(self, q_mark:str='24Q4', industry_id:str='-1'):
        """
        >>> Q MARK: 24Q4, 23Q2, etc..
        """
        url=f"https://www.moomoo.com/quote-api/quote-v2/get-industry-stock-report-list?qMark={q_mark}&industryId={industry_id}&moreMark="
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers) as resp:

                data = await resp.json()
                data = data['data']
                cards = data['cards']
                return EarningsData(cards)