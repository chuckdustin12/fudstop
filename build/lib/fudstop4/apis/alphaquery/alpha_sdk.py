import pandas as pd
import asyncio
import httpx
from bs4 import BeautifulSoup




class AlphaSDK:
    def __init__(self):
        pass


    async def make_table(self, data):

        if 'resultsHtml' in data:
            results_html = data['resultsHtml']

            # Use BeautifulSoup to parse the HTML content
            soup = BeautifulSoup(results_html, 'lxml')

            # Find the table by its class or id
            table = soup.find('table', class_='query-results')

            # Extract the table header
            columns = [th.text.strip() for th in table.find('thead').find_all('th')]

            # Initialize a list to store all rows of the table
            data_rows = []

            # Iterate over each row of the table
            for row in table.find('tbody').find_all('tr'):
                # Extract text from each cell of the row and strip any leading/trailing whitespace
                row_data = [td.text.strip() for td in row.find_all('td')]
                data_rows.append(row_data)


            return pd.DataFrame(data_rows, columns=columns)


    async def core_puts(self):

        endpoint = "https://www.alphaquery.com/service/run-screen?a=2ff5a75f9db242481681b799b1c3e18ff3f2c4ba9488585253e194667b80779f&screen=%5B%7B%22columnName%22%3A%22sector%22%2C%22operator%22%3A%22is%20not%22%2C%22value%22%3A%22Healthcare%22%2C%22valueType%22%3A%22%22%2C%22unit%22%3A%22%22%7D%2C%7B%22columnName%22%3A%22rsi_14%22%2C%22operator%22%3A%22is%20greater%20than%22%2C%22value%22%3A%2270%22%2C%22valueType%22%3A%22number%22%2C%22unit%22%3A%22%22%7D%2C%7B%22columnName%22%3A%22days_since_report_date_qr0%22%2C%22operator%22%3A%22is%20less%20than%22%2C%22value%22%3A%2214%22%2C%22valueType%22%3A%22number%22%2C%22unit%22%3A%22%22%7D%2C%7B%22columnName%22%3A%22close_price%22%2C%22operator%22%3A%22is%20greater%20than%22%2C%22value%22%3A%2210%22%2C%22valueType%22%3A%22number%22%2C%22unit%22%3A%22%22%7D%2C%7B%22columnName%22%3A%22historical_volatility_10%22%2C%22operator%22%3A%22is%20greater%20than%22%2C%22value%22%3A%220%22%2C%22valueType%22%3A%22number%22%2C%22unit%22%3A%22%22%7D%5D"



        async with httpx.AsyncClient() as client:
            data = await client.get(endpoint)

            data = data.json()

            data = await self.make_table(data)

            return data


    async def core_calls(self):

        endpoint = "https://www.alphaquery.com/service/run-screen?a=2ff5a75f9db242481681b799b1c3e18ff3f2c4ba9488585253e194667b80779f&screen=%5B%7B%22columnName%22%3A%22sector%22%2C%22operator%22%3A%22is%20not%22%2C%22value%22%3A%22Healthcare%22%2C%22valueType%22%3A%22%22%2C%22unit%22%3A%22%22%7D%2C%7B%22columnName%22%3A%22close_price%22%2C%22operator%22%3A%22is%20greater%20than%22%2C%22value%22%3A%2215%22%2C%22valueType%22%3A%22number%22%2C%22unit%22%3A%22%22%7D%2C%7B%22columnName%22%3A%22rsi_14%22%2C%22operator%22%3A%22is%20less%20than%22%2C%22value%22%3A%2230%22%2C%22valueType%22%3A%22number%22%2C%22unit%22%3A%22%22%7D%2C%7B%22columnName%22%3A%22days_since_report_date_qr0%22%2C%22operator%22%3A%22is%20less%20than%22%2C%22value%22%3A%228%22%2C%22valueType%22%3A%22number%22%2C%22unit%22%3A%22%22%7D%2C%7B%22columnName%22%3A%22historical_volatility_10%22%2C%22operator%22%3A%22is%20greater%20than%22%2C%22value%22%3A%220%22%2C%22valueType%22%3A%22number%22%2C%22unit%22%3A%22%22%7D%5D"



        async with httpx.AsyncClient() as client:
            data = await client.get(endpoint)

            data = data.json()

            data = await self.make_table(data)

            return data





