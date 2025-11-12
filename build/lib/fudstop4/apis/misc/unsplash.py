import asyncio
import os

import aiohttp

from dotenv import load_dotenv

key = os.environ.get('YOUR_UNSPLASH_ACCESS_KEY')

from .models.unsplash_models import ImageURLS

class Unsplash:
    def __init__(self):
        self.key = key



    async def get_image(self, query:str):
        """
        Fetches the UNSPLASH API and returns an image based on user query.


        Arguments:

        >>> query: the image to query
        
        """

        url = f"https://api.unsplash.com/search/photos?page=1&query={query}&client_id={self.key}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                results = data['results']
                urls = [i.get('urls') for i in results]

                urls = urls[0]

                return ImageURLS(urls)







