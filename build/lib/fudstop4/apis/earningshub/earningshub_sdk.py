
import pandas as pd
import aiohttp
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
opts = PolygonOptions()
import uuid
from fudstop4.apis.helpers import camel_to_snake_case

from .models import EarningsCalendar, News

class EarningsHubSDK:
    def __init__(self):

        self.headers = { 
            'Accesstoken': 'eyJraWQiOiJWSk44ellOcmRiK1YzbWZUd0RHZDg5eTJTQ1R0V0JEMXNLVGRIRUdTZFhnPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiI0ODUyOTM2Yi02ZDVmLTRmNjgtODgxYy1jMzMxZTJjYmQwOWEiLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0xLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMV9kSHlsU1NSWlciLCJ2ZXJzaW9uIjoyLCJjbGllbnRfaWQiOiI0bTc1YzBrdGIxczBsb3Bub25mMTV2ajY3cCIsIm9yaWdpbl9qdGkiOiJhMGZiZGQ3OS1mNzE3LTQ3MmItOWY5YS1iODEwY2JkMjY1YjAiLCJ0b2tlbl91c2UiOiJhY2Nlc3MiLCJzY29wZSI6ImF3cy5jb2duaXRvLnNpZ25pbi51c2VyLmFkbWluIG9wZW5pZCBwcm9maWxlIGVtYWlsIiwiYXV0aF90aW1lIjoxNzM4OTg2ODEyLCJleHAiOjE3Mzg5OTA0MTIsImlhdCI6MTczODk4NjgxMiwianRpIjoiMThmNjE2NTktNzk1My00ZjdiLWI0NDUtM2Y2MWQzNzNiN2Y4IiwidXNlcm5hbWUiOiI0ODUyOTM2Yi02ZDVmLTRmNjgtODgxYy1jMzMxZTJjYmQwOWEifQ.FTsJr7edlfsnOjg4Ov_EhAGVGgSXgGUg1NdRiFoOg2WhqiQg7gf7NKGXci9RPZlmmqwv05SSILRZobd_BMfgkV5q74K5RSMx7MZJXWXalZQGF5X7eTVko_HxRC8Wcfur2NW-S8Zhn79R-7k1qKrv5XF58MFfuZHjhsfTbz2cN8FrKGcyqg8aFvZlR8IpeumN3Qf5E-8EaZw6xp_3mZSrvptI5QNYbD4eU8wg_u5h6Y5TaVKgUOSE6jkQZkyTpMNy9BZSi9sp5xsFm9z6P_lVq4LLMjSTeUJp0d31AYfiqqTYh4q2TSDcJpMnIDBCq3gIv9oWwpn9ChrzbJTb6xCk-A',
            'Authorization': 'Bearer eyJraWQiOiJiUmZ5VzkrYzZITFhXZ0dlaUtkYUVYK2FFU21CWko1SkZqS2xvZGJnanhVPSIsImFsZyI6IlJTMjU2In0.eyJhdF9oYXNoIjoiTVRocGV5dC1sc0Ntd1pnV2NfZ3hKZyIsInN1YiI6IjQ4NTI5MzZiLTZkNWYtNGY2OC04ODFjLWMzMzFlMmNiZDA5YSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0xLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMV9kSHlsU1NSWlciLCJjb2duaXRvOnVzZXJuYW1lIjoiNDg1MjkzNmItNmQ1Zi00ZjY4LTg4MWMtYzMzMWUyY2JkMDlhIiwiZ2l2ZW5fbmFtZSI6IkZVRFNUT1AiLCJub25jZSI6ImYxaFI2WXZ2OFlpMjhqRDFmTURtU2pPU3FYLTFhQ3kwc0ZWRU1wNXBlcVpoVDk3bTlqNHRNQ2FTVzY1QzRZeTFrSDVNenpuQ0tCMUViR3V4OFQxbkFqYllSQWpjMHljdXRIQnNzd1Fab2tJN19qRUhTT0xWX1RGY2NNOS1WM0hEQ3NoXzVjd0lzSGdwOHBTdGZBUEdCRDIzYWJGbWpxNW5HdEJidHEza24zbyIsIm9yaWdpbl9qdGkiOiIyNjJiMTZhOS0wMjY3LTQxZWYtODYyOC0yNjEwYTA3ODMwNDYiLCJhdWQiOiI0bTc1YzBrdGIxczBsb3Bub25mMTV2ajY3cCIsImlkZW50aXRpZXMiOlt7InVzZXJJZCI6IjEwNTU4NzQ3NzUwNzQxNDM0NjU4MiIsInByb3ZpZGVyTmFtZSI6Ikdvb2dsZSIsInByb3ZpZGVyVHlwZSI6Ikdvb2dsZSIsImlzc3VlciI6bnVsbCwicHJpbWFyeSI6ImZhbHNlIiwiZGF0ZUNyZWF0ZWQiOiIxNzM4OTg2NTQ4MDg0In1dLCJ0b2tlbl91c2UiOiJpZCIsImF1dGhfdGltZSI6MTczODk4NjU0OCwiZXhwIjoxNzM4OTkwMTQ4LCJpYXQiOjE3Mzg5ODY1NDgsImp0aSI6IjMxNDMwNjkzLTExMTMtNDYxNS05Mzg4LTg0YTE0OTBlNzE3NSIsImVtYWlsIjoiY2h1Y2tkdXN0aW4xMkBnbWFpbC5jb20ifQ.LA9cy2I-qpMnvW5FUI19nuXgixZreVrn0PTztM71BZZKcF6DxuFG9zrPGouqHW0bbSEMat-TSl4XP8q0iCdgvTyLO7EgOORpSe5BymQP_ydQlrg0ssX8B86iPYODWe0VbdLr5PHB6BrPt1n5QwqIXSw5F_6r1iPtSN8rkfDO1cJjzHXRZWndKxQKVHTsMVWFNEDRoZYDqjfdG_GMyi3P-_bREhkCa_5ItQuzzFJy2vqcAXeU08y0YZPEkqxuFJnk7engh9UuXtWuNtLNTB1MlNdQVrS6jn7IyyjP8Ue9t8LCv4e9F93zyDJdM6XYUc2IDFBUqPFNX7DjxYG2V3qn0g',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
        }


    async def news(self, limit:str='50'):
        url = f"https://api.savvytrader.com/pricing/news?filter=popular&types=headline%2Cstory&limit={limit}"


        data = await self.fetch_data(url, params={'limit': limit})


        return News(data)


    async def fetch_data(self, url, params=None, return_json=True):
        """
        Dynamically fetch data from a given URL using an HTTP GET request.

        Args:
            url (str): The target URL.
            params (dict, optional): URL parameters to be sent with the GET request.
            headers (dict, optional): HTTP headers to send with the request.
            return_json (bool, optional): If True, returns JSON-decoded data; otherwise, returns text.
        
        Returns:
            dict or str: The response data in JSON format (if return_json is True) or as text.
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=self.headers) as response:
                if return_json:
                    return await response.json()
                return await response.text()


    async def earnings_calendar(self, start=None, end=None):
        if start is None:
            start = opts.today
        if end is None:
            end = opts.eight_days_from_now

        url = "https://api.savvytrader.com/pricing/assets/earnings/calendar/daily"
        params = {"start": start, "end": end}
        
        # Fetch the data asynchronously.
        data = await self.fetch_data(url, params=params)
                
        # Flatten the JSON structure.
        flattened_data = []
        for date, records in data.items():
            for record in records:
                # Optionally, if you want to ensure the record's date is consistent,
                # you can overwrite or add the 'date' key.
                record['date'] = date
                flattened_data.append(record)



        return EarningsCalendar(flattened_data)
    


    async def economic_calendar(self, start=None, end=None):
        if start == None:
            start = opts.today
        
        if end == None:
            end = opts.thirty_days_from_now

        url = f"https://api.savvytrader.com/events/?start=2025-02-10&end=2025-02-14&type="
        params = {"start": start, "end": end}
        
        # Fetch the data asynchronously.
        data = await self.fetch_data(url, params=params)
        print(data)