import pandas as pd
import httpx
import os
from dotenv import load_dotenv
load_dotenv()
from .rapid_models import HomeSearch
ezuid = os.environ.get('zillow_ezuid')
abKey = os.environ.get('zillow_abKey')
account_id = os.environ.get('zillow_account_id')
zone_id = os.environ.get('zone_id')


class RapidSDK:
    def __init__(self):

        self.key=os.environ.get('YOUR_RAPIDAPI_KEY')
        self.headers = {
            "content-type": "application/json",
            "X-RapidAPI-Key": "cd36127562msh09bad643a11b69cp16a83cjsnce170739c830",
            "X-RapidAPI-Host": "realty-in-us.p.rapidapi.com"
        }

    async def search_homes(self, area:str='Watauga', limit:str='10'):

        """"""

        endpoint = f"https://realty-in-us.p.rapidapi.com/locations/v2/auto-complete?input={area}&limit={limit}"
        



        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint, headers=self.headers)
            if data.status_code == 200:
                return data.json()
     


    async def fetch_properties(self, limit=200, offset=0, postal_code="76148", status_list=None, sort_direction="desc", sort_field="list_date"):
        """
        Asynchronously fetches real estate properties based on specified criteria from a realty API.

        Parameters:
        - limit (int): The number of properties to retrieve (default is 200).
        - offset (int): The offset from which to start retrieving properties (default is 0).
        - postal_code (str): The postal code to filter properties (default is "90004").
        - status_list (list): The list of statuses to filter properties. Defaults to ["for_sale", "ready_to_build"].
        - sort_direction (str): The direction to sort the results ("asc" for ascending or "desc" for descending, default is "desc").
        - sort_field (str): The field to sort the results by (default is "list_date").

        Returns:
        - dict: A JSON object containing the fetched property data.
        """
        # Default value for status_list if not provided
        if status_list is None:
            status_list = ['for_sale']

        url = "https://realty-in-us.p.rapidapi.com/properties/v3/list"
        payload = {
            "limit": limit,
            "offset": offset,
            "postal_code": postal_code,
            "status": ['for_sale'],
            "sort": {
                "direction": sort_direction,
                "field": sort_field
            }
        }
        headers = {
            "content-type": "application/json",
            "X-RapidAPI-Key": os.environ.get('YOUR_RAPIDAPI_KEY'),
            "X-RapidAPI-Host": "realty-in-us.p.rapidapi.com"
        }

        async with httpx.AsyncClient() as client:
            data = await client.post(url=url, json=payload, headers=self.headers)
            data = data.json()
            data = data['data']
            home_search = data['home_search']
            results = home_search['results'] if 'results' in home_search else None
            if results is not None:
                return HomeSearch(results)