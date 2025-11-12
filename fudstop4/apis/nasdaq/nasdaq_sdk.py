import sys
import os
import aiohttp
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from typing import List
from .models import EarningsData, Dividends, EconomicEvents, NasdaqScreener, Insiders, GetNews
from .nasdaq_lists import country_acronyms, repo_codes, sp500_codes, income_expenditure_codes, economic_indicator_codes, unemployment_codes, interest_rate_codes, gdp_codes
import pandas as pd
import requests
import io
session = requests.session()
class Nasdaq:
    def __init__(self):
        self.api_key = os.environ.get("YOUR_NASDAQ_KEY")
        self.base_url = "https://data.nasdaq.com/api/v3/datasets/"
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",

            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        }


    def bigmac(self):
        """Download Big Mac valuation data per country.

        Args:
            country_acronyms (List[str]): List of country acronyms to download data for.

        Returns:
            A csv file in the corresponding directory.
        """
        session = requests.Session()

        for country in country_acronyms:
            r = session.get(f"{self.base_url}ECONOMIST/BIGMAC_{country}.csv?&api_key={self.api_key}")

            # Check if the request was successful
            if r.status_code == 200:
                # Create a file-like object from the response content
                file_like_object = io.BytesIO(r.content)

                df = pd.read_csv(file_like_object)
                directory = 'files/nasdaq/bigmac/'

                # Create the directory if it doesn't exist
                if not os.path.exists(directory):
                    os.makedirs(directory)

                # Save the DataFrame to a separate CSV file for each country
                filename = f'{directory}{country}bigmac_data.csv'
                df.to_csv(filename, index=False)
                print(f"Saved data for {country} to {filename}")
            else:
                print(f"Error downloading CSV for {country}: {r.status_code}")

    def inflation(self):
        """Download inflation data across the globe.

        Args:
            country_acronyms (List[str]): List of country acronyms to download data for.

        Returns:
            A csv file in the corresponding directory.
        """
        session = requests.Session()

        for country in country_acronyms:
            r = session.get(f"{self.base_url}RATEINF/CPI_{country}.csv?&api_key={self.api_key}")

            # Check if the request was successful
            if r.status_code == 200:
                # Create a file-like object from the response content
                file_like_object = io.BytesIO(r.content)

                df = pd.read_csv(file_like_object)
                directory = 'files/nasdaq/inflation/'

                # Create the directory if it doesn't exist
                if not os.path.exists(directory):
                    os.makedirs(directory)

                # Save the DataFrame to a separate CSV file for each country
                filename = f"{directory}{country}inflation.csv"
                df.to_csv(filename, index=False)
                print(f"Saved data for {country} to {filename}")
            else:
                print(f"Error downloading CSV for {country}: {r.status_code}")

    def repo(self):
        """Download repo data from the Federal Reserve.

        Args:
            repo_codes (List[str]): List of repo codes to download data for.

        Returns:
            A csv file in the corresponding directory.
        """
        session = requests.Session()

        for repo_code in repo_codes:
            r = session.get(f"{self.base_url}FRED/{repo_code}.csv?&api_key={self.api_key}")

            # Check if the request was successful
            if r.status_code == 200:
                # Create a file-like object from the response content
                file_like_object = io.BytesIO(r.content)

                df = pd.read_csv(file_like_object)
                directory = 'files/nasdaq/repo/'

                # Create the directory if it doesn't exist
                if not os.path.exists(directory):
                    os.makedirs(directory)

                # Save the DataFrame to a separate CSV file for each repo code
                filename = f"{directory}{repo_code}_repo.csv"
                df.to_csv(filename, index=False)
                print(f"Saved data for {repo_code} to {filename}")
            else:
                print(f"Error downloading CSV for {repo_code}: {r.status_code}")



    def sp500(self):
        """Download SP500 macro data from the Federal Reserve.

        Args:
            sp500_codes (List[str]): List of SP500 codes to download data for.

        Returns:
            A csv file in the corresponding directory.
        """
        session = requests.Session()

        for sp500_code in sp500_codes:
            r = session.get(f"{self.base_url}MULTPL/{sp500_code}.csv?&api_key={self.api_key}")

            # Check if the request was successful
            if r.status_code == 200:
                # Create a file-like object from the response content
                file_like_object = io.BytesIO(r.content)

                df = pd.read_csv(file_like_object)
                directory = 'files/nasdaq/sp_500/'

                # Create the directory if it doesn't exist
                if not os.path.exists(directory):
                    os.makedirs(directory)

                # Save the DataFrame to a separate CSV file for each SP500 code
                filename = f"{directory}{sp500_code}.csv"
                df.to_csv(filename, index=False)
                print(f"Saved data for {sp500_code} to {filename}")
            else:
                print(f"Error downloading CSV for {sp500_code}: {r.status_code}")

    def income_expenditures(self):
        """Download income expenditure data from the Federal Reserve.

        Args:
            income_expenditure_codes (List[str]): List of income expenditure codes to download data for.

        Returns:
            A csv file in the corresponding directory.
        """
        session = requests.Session()

        for income_expenditure_code in income_expenditure_codes:
            r = session.get(f"{self.base_url}FRED/{income_expenditure_code}.csv?&api_key={self.api_key}")

            # Check if the request was successful
            if r.status_code == 200:
                # Create a file-like object from the response content
                file_like_object = io.BytesIO(r.content)

                df = pd.read_csv(file_like_object)
                directory = 'files/nasdaq/income_expenditures/'

                # Create the directory if it doesn't exist
                if not os.path.exists(directory):
                    os.makedirs(directory)

                # Save the DataFrame to a separate CSV file for each income expenditure code
                filename = f"{directory}{income_expenditure_code}.csv"
                df.to_csv(filename, index=False)
                print(f"Saved data for {income_expenditure_code} to {filename}")
            else:
                print(f"Error downloading CSV for {income_expenditure_code}: {r.status_code}")

    def economic_indicators(self):
        """Download economic indicator data from the Federal Reserve.

        Args:
            economic_indicator_codes (List[str]): List of economic indicator codes to download data for.

        Returns:
            A csv file in the corresponding directory.
        """
        session = requests.Session()

        for economic_indicator_code in economic_indicator_codes:
            r = session.get(f"{self.base_url}FRED/{economic_indicator_code}.csv?&api_key={self.api_key}")

            # Check if the request was successful
            if r.status_code == 200:
                # Create a file-like object from the response content
                file_like_object = io.BytesIO(r.content)

                df = pd.read_csv(file_like_object)
                directory = 'files/nasdaq/economic_indicators/'

                # Create the directory if it doesn't exist
                if not os.path.exists(directory):
                    os.makedirs(directory)

                # Save the DataFrame to a separate CSV file for each economic indicator code
                filename = f"{directory}{economic_indicator_code}.csv"
                df.to_csv(filename, index=False)
                print(f"Saved data for {economic_indicator_code} to {filename}")
            else:
                print(f"Error downloading CSV for {economic_indicator_code}: {r.status_code}")


    def unemployment(self):
        """Return unemeployment data from the FED
        
        
        Returns: A csv file in the corresponding directory.
        """
        for unemployment_code in unemployment_codes:
            r = session.get(f"{self.base_url}FRED/{unemployment_code}.csv?&api_key={self.api_key}")



            # Check if the request was successful
            if r.status_code == 200:
                # Create a file-like object from the response content
                file_like_object = io.BytesIO(r.content)

                df = pd.read_csv(file_like_object)
                directory = 'files/nasdaq/unemployment/'

                # Create the directory if it doesn't exist
                if not os.path.exists(directory):
                    os.makedirs(directory)
                # Save the DataFrame to a separate CSV file for each country

                filename = f"{directory}{unemployment_code}_repo.csv"
                df.to_csv(filename, index=False)
                print(f"Saved data for {unemployment_code} to {filename}")

            else:
                print(f"Error downloading CSV for {unemployment_code}: {r.status_code}")



    def interest_rates(self):
        """Download interest rate data for the specified interest rate codes.

        Args:
            interest_rate_codes (List[str]): List of interest rate codes to download data for.

        Returns:
            None
        """
        session = requests.Session()

        for interest_rate_code in interest_rate_codes:
            r = session.get(f"{self.base_url}FRED/{interest_rate_code}.csv?&api_key={self.api_key}")

            # Check if the request was successful
            if r.status_code == 200:
                # Create a file-like object from the response content
                file_like_object = io.BytesIO(r.content)

                df = pd.read_csv(file_like_object)
                directory = 'files/nasdaq/interest_rates/'

                # Create the directory if it doesn't exist
                if not os.path.exists(directory):
                    os.makedirs(directory)

                # Save the DataFrame to a separate CSV file for each interest rate code
                filename = f"{directory}{interest_rate_code}_repo.csv"
                df.to_csv(filename, index=False)
                print(f"Saved data for {interest_rate_code} to {filename}")
            else:
                print(f"Error downloading CSV for {interest_rate_code}: {r.status_code}")

    def gdp(self):
        """Download gross domestic product (GDP) data for the specified GDP codes.

        Args:
            gdp_codes (List[str]): List of GDP codes to download data for.

        Returns:
            None
        """
        session = requests.Session()

        for gdp_code in gdp_codes:
            r = session.get(f"{self.base_url}FRED/{gdp_code}.csv?&api_key={self.api_key}")

            # Check if the request was successful
            if r.status_code == 200:
                # Create a file-like object from the response content
                file_like_object = io.BytesIO(r.content)

                df = pd.read_csv(file_like_object)
                directory = 'files/nasdaq/gdp/'

                # Create the directory if it doesn't exist
                if not os.path.exists(directory):
                    os.makedirs(directory)

                # Save the DataFrame to a separate CSV file for each GDP code
                filename = f"{directory}{gdp_code}_repo.csv"
                df.to_csv(filename, index=False)
                print(f"Saved data for {gdp_code} to {filename}")
            else:
                print(f"Error downloading CSV for {gdp_code}: {r.status_code}")


    async def earnings(self, date:str):

        url = f"https://api.nasdaq.com/api/calendar/earnings?date=2025-01-15"

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url) as response:
                data = await response.json()

                data = data['data']
                headers = data['headers']
                rows = data['rows']

                earnings_data = EarningsData(rows)


                return earnings_data
            

    async def dividends(self, date:str):

        url = f"https://api.nasdaq.com/api/calendar/dividends?date={date}"

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url) as response:
                data = await response.json()

                data = data['data']
                calendar = data['calendar']
                rows = calendar['rows']


                dividend_data = Dividends(rows)

                return dividend_data
            

    async def economic_events(self, date:str):
        url = f"https://api.nasdaq.com/api/calendar/economicevents?date={date}"

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url) as response:
                data = await response.json()
                data=data['data']

                rows = data['rows']

                return EconomicEvents(rows)

    async def screener(self):

        url = f"https://api.nasdaq.com/api/screener/stocks?tableonly=false&limit=25&marketcap=mega&recommendation=strong_buy"


        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url) as response:
                data = await response.json()
                print(data)



    async def screener(self, 
                       table_only=False, 
                       limit=25, 
                       exchange="NASDAQ", 
                       exsubcategory=None, 
                       marketcap=None, 
                       recommendation=None, 
                       sector=None):
        """
        Fetch stock data from the NASDAQ screener API.

        :param table_only: Boolean indicating whether to fetch only the table data.
        :param limit: Maximum number of results to return.
        :param exchange: Exchange to query (e.g., "NASDAQ").
        :param exsubcategory: Exchange subcategory filter (e.g., "ADR").
        :param marketcap: Market capitalization filter, one or more of:
                          ["mega", "large", "mid", "small", "micro", "nano"].
        :param recommendation: Recommendation filter, one or more of:
                               ["strong_buy", "buy", "hold", "sell", "strong_sell"].
        :param sector: Sector filter, one or more of:
                       ["technology", "telecommunications", "finance", "health_care",
                        "real_estate", "consumer_discretionary", "industrials",
                        "consumer_staples", "basic_materials", "utilities", "energy"].

        :return: JSON data from the NASDAQ screener API.
        """
        base_url = "https://api.nasdaq.com/api/screener/stocks"
        params = {
            "tableonly": str(table_only).lower(),  # Convert boolean to string ("true"/"false")
            "limit": limit,
            "exchange": exchange,
            "exsubcategory": exsubcategory,
            "marketcap": "|".join(marketcap) if marketcap else None,
            "recommendation": "|".join(recommendation) if recommendation else None,
            "sector": "|".join(sector) if sector else None,
        }

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(base_url, params={k: v for k, v in params.items() if v}) as response:
                if response.status == 200:
                    data = await response.json()
                    data = data['data']

                    table = data['table']
                    rows = table['rows']
                    screener = NasdaqScreener(rows)
                    return screener
                else:
                    response.raise_for_status()


    async def insiders(self, limit:str='25'):

        url = f"https://api.nasdaq.com/api/company/search-ownership?&type=2&searchonly=false&limit={limit}"
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    data = data['data']
                    table = data['table']
                    rows = table['rows']
                    return Insiders(rows)
                

    async def get_secret_indicator(self, ticker):

        url = f"https://api.nasdaq.com/api/quote/{ticker}/info?assetclass=stocks"
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url) as response:
                data = await response.json()

                data = data['data']

                primaryData = data['primaryData']

                delta_indicator = primaryData['deltaIndicator']
                return delta_indicator


    async def get_news(self):

        url = f"https://www.nasdaq.com/api/news/topic/article?q=field_primary_topic:4286&limit=100&offset=0"

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url) as response:
                data = await response.json()

                data = data['data']

                rows = data['rows']

                return GetNews(rows)