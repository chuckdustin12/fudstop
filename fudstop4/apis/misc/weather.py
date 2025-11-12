import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)


import aiohttp
import asyncio
import os

from apis.misc.models.weather_models import WeatherLocation

API_KEY = os.environ.get('YOUR_WEATHER_KEY')



class Weather:
    def __init__(self):
        self.key = API_KEY



    async def get_coordinates(self, city_name, state, country_code, limit:str='2'):
        url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name},{state},{country_code}&limit={limit}&appid={API_KEY}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()
                if data:
                    return data[0]['lat'], data[0]['lon']
                else:
                    return None, None
                


    async def get_weather(self, city, state, country_code, limit:str='5', unit:str='F'):
        """
        Get the weather for the provided location.

        Example:

        >>> Dallas, TX, USA

        >>> boise, idaho, us

        >>> dallas texas, us

        >>> Sacramento, CA, usa

        Optional Args:


        >>> limit: the number of days of weather to return || default 5
        
        
        """

        lat, lon = await self.get_coordinates(city,state,country_code,limit)
        url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude=minutely,hourly,daily,alerts&appid={self.key}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                return WeatherLocation(data, unit=unit)


