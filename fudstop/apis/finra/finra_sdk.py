import pandas as pd
import httpx
from .models.finra_models import TickerATS
from fudstop.apis.polygonio.polygon_database import PolygonDatabase
from fudstop.apis.helpers import format_large_numbers_in_dataframe
db = PolygonDatabase()
import os
import asyncio
import httpx
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
# Load environment variables from .env file
load_dotenv()

# Retrieve the access token from the .env file
access_token = os.getenv("FINRA_ACCESS")

if not access_token:
    raise ValueError("Access token not found in .env file. Please check 'FINRA_ACCESS'.")
import asyncio

class FinraSDK:
    def __init__(self):
        self.headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        }

        self.download_info_payload = {"quoteValues":False,"delimiter":"|","limit":500,"fields":["weekStartDate"],"sortFields":["-weekStartDate"],"compareFilters":[{"fieldName":"summaryTypeCode","fieldValue":"ATS_W_FIRM","compareType":"EQUAL"},{"fieldName":"tierIdentifier","fieldValue":"T1","compareType":"EQUAL"}]}


    async def download_details(self):
        endpoint = f"https://api.finra.org/data/group/otcMarket/name/weeklyDownloadDetails"

        payload = self.download_info_payload
        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.post(endpoint,json=payload, headers=self.headers)


            data = data.json()

            week_starts = [i.get('weekStartDate') for i in data]
            

            return week_starts


    async def summary(self, type:str='WEEKLY'):
        endpoint=f"https://api.finra.org/data/group/OTCMARKET/name/{type}"
        payload={"quoteValues":False,"delimiter":"|","limit":1000,"fields":[],"compareFilters":[{"fieldName":"weeklyStartDate","fieldValue":"2024-05-13","compareType":"EQUAL"}]}


        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.post(endpoint,json=payload, headers=self.headers)


            data = data.json()

            print(data)


    async def ticker_ats(self, ticker:str, week_start:str='2024-05-13'):
        try:
            payload = {"quoteValues":False,"delimiter":"|","limit":5000,"sortFields":["-totalWeeklyShareQuantity"],"compareFilters":[{"fieldName":"summaryTypeCode","fieldValue":"ATS_W_SMBL_FIRM","compareType":"EQUAL"},{"fieldName":"issueSymbolIdentifier","fieldValue":ticker,"compareType":"EQUAL"},{"fieldName":"weekStartDate","fieldValue":week_start,"compareType":"EQUAL"},{"fieldName":"tierIdentifier","fieldValue":"T1","compareType":"EQUAL"}]}

            endpoint=f"https://api.finra.org/data/group/otcMarket/name/weeklySummary"

            async with httpx.AsyncClient(headers=self.headers) as client:
                data = await client.post(endpoint,json=payload, headers=self.headers)


                data = data.json()


                data = TickerATS(data)
                return data
        except Exception as e:
            print(e)
            
        async with httpx.AsyncClient(headers=self.headers) as client:
            response = await client.post(endpoint, json=payload)

            try:
                response.raise_for_status()  # Raise an HTTPError if the response was an HTTP error
                data = response.json()
            except httpx.HTTPStatusError as exc:
                print(f"HTTP error occurred: {exc.response.status_code} - {exc.response.text}")
                return None
            except ValueError as exc:
                print(f"JSON decoding error: {exc}")
                return None

            return TickerATS(data)
        
    async def ticker_nonats(self, ticker:str, week_start:str='2024-05-20'):
        try:
            payload={"quoteValues":True,"delimiter":"|","limit":5000,"sortFields":["-totalWeeklyShareQuantity"],"compareFilters":[{"fieldName":"summaryTypeCode","fieldValue":"OTC_W_SMBL_FIRM","compareType":"EQUAL"},{"fieldName":"issueSymbolIdentifier","fieldValue":f"{ticker}","compareType":"EQUAL"},{"fieldName":"issueName","fieldValue":"GameStop Corp. Class A","compareType":"EQUAL"},{"fieldName":"tierIdentifier","fieldValue":"T1","compareType":"EQUAL"},{"fieldName":"weekStartDate","fieldValue":week_start,"compareType":"EQUAL"}]}

            endpoint = f"https://api.finra.org/data/group/otcMarket/name/weeklySummary"
            async with httpx.AsyncClient(headers=self.headers) as client:
                data = await client.post(endpoint, json=payload)

                data = data.json()


                data = TickerATS(data)
                return data
        except Exception as e:
            print(e)
    async def all_ticker_ats(self, ticker: str):

        await db.connect()
        try:
            dates = await self.download_details()

            tasks = [self.ticker_ats(ticker, date) for date in dates]
            results = await asyncio.gather(*tasks)

            # Filter out None results in case of errors
            results = [result for result in results if result is not None]

            # Convert each result to a DataFrame and concatenate them
            dataframes = [result.as_dataframe for result in results]
            final_df = pd.concat(dataframes, ignore_index=True)
            final_df = final_df.rename(columns={'issue_symbol_identifier': 'ticker', 'market_participant_name': 'participant'})
            await db.batch_insert_dataframe(final_df, table_name='finra_ats', unique_columns='ticker, last_reported_date')
            final_df = final_df.drop(columns=['firm_crd_number', 'product_type_code', 'summary_type_code'])
            print(final_df.columns)
            return final_df
        except Exception as e:
            print(f"An error occurred: {e}")


    async def all_ticker_nonats(self, ticker:str):
        await db.connect()
        try:
            dates = await self.download_details()
            tasks = [self.ticker_nonats(ticker,date) for date in dates]
            results = await asyncio.gather(*tasks)
            results = [result for result in results if result is not None]
            dataframes = [result.as_dataframe for result in results]
            final_df = pd.concat(dataframes, ignore_index=True)
            final_df = final_df.rename(columns={'issue_symbol_identifier': 'ticker', 'market_participant_name': 'participant'})
            await db.batch_insert_dataframe(final_df, table_name='finra_nonats', unique_columns='ticker, last_reported_date')
            final_df = final_df.drop(columns=['firm_crd_number', 'product_type_code', 'summary_type_code'])
            print(final_df.columns)
            return final_df
        except Exception as e:
            print(f"An error occurred: {e}")





    async def fetch_monthly_data(self, limit: int = 5000, week_start_date: str = '2024-12-01', tier:str='T1'):
        """
        Get Monthly OTC data. Ensures the week_start_date is a Monday.
        
        Args:
            limit (int): Maximum number of records to fetch.
            week_start_date (str): The starting date in yyyy-MM-dd format. Adjusts to the most recent Monday if not provided or if not a Monday.


        TIERS:

          >>> T1 : tier 1
          >>> T2 : tier 2
          >>> OTCE: otc
        """
        base_url = "https://api.finra.org"
        
        # Calculate the current month in yyyy-MMM format
        today = datetime.utcnow()
        historical_month = today.strftime("%Y-%b")
        print(f"Querying historicalMonth: {historical_month}")
        
        # Validate or calculate week_start_date
        if week_start_date is None:
            # Default to the most recent Monday
            days_since_monday = today.weekday()  # Monday is 0
            adjusted_date = today - timedelta(days=days_since_monday)
        else:
            # Convert the provided date string to a datetime object
            provided_date = datetime.strptime(week_start_date, "%Y-%m-%d")
            if provided_date.weekday() != 0:  # Not a Monday
                # Adjust to the most recent Monday
                days_since_monday = provided_date.weekday()
                adjusted_date = provided_date - timedelta(days=days_since_monday)
            else:
                adjusted_date = provided_date
        # Define dataset details
        group_name = "otcMarket"
        dataset_name = "weeklysummary"
        endpoint = f"/data/group/{group_name}/name/{dataset_name}"
        # Format the adjusted date as yyyy-MM-dd
        adjusted_week_start_date = adjusted_date.strftime("%Y-%m-%d")
        print(f"Using week_start_date: {adjusted_week_start_date}")

        # Headers for the API request
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            'Accept-Encoding': 'gzip, deflate, br, zstd'
        }
    
        # Query parameters (using only one field)
        params = {"quoteValues":True,"delimiter":"|","limit":limit,"fields":["productTypeCode","issueSymbolIdentifier","issueName","totalWeeklyShareQuantity","totalWeeklyTradeCount","lastUpdateDate"],"compareFilters":[{"fieldName":"weekStartDate","fieldValue":adjusted_week_start_date,"compareType":"EQUAL"},{"fieldName":"tierIdentifier","fieldValue":tier,"compareType":"EQUAL"},{"fieldName":"summaryTypeCode","description":None,"fieldValue":"ATS_W_SMBL","compareType":"EQUAL"}]}
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post("https://api.finra.org/data/group/otcMarket/name/weeklySummary", headers=headers, json=params)
                if response.status_code == 200:
                    data = response.json()

                    totalWeekly = [i.get('totalWeeklyTradeCount') for i in data]
                    symbol = [i.get('issueSymbolIdentifier') for i in data]
                    name = [i.get('issueName') for i in data]
                    last_updated = [i.get('lastUpdateDate') for i in data]
                    product_code = [i.get('productTypeCode') for i in data]

                    dict = { 
                        'total_weekly': totalWeekly,
                        'ticker': symbol,
                        'name': name,
                        'last_updated': last_updated,
                        'code': product_code
                    }

                    df = pd.DataFrame(dict)

                    return df

            except httpx.RequestError as exc:
                print(f"An error occurred: {exc}")
                return None



    async def ats_summary(self, week_start_date:str='2024-01-01', tier:str='T1'):
        """        TIERS:

          >>> T1 : tier 1
          >>> T2 : tier 2
          >>> OTCE: otc
        """
        # Calculate the current month in yyyy-MMM format
        today = datetime.utcnow()
        historical_month = today.strftime("%Y-%b")
        print(f"Querying historicalMonth: {historical_month}")
        
        # Validate or calculate week_start_date
        if week_start_date is None:
            # Default to the most recent Monday
            days_since_monday = today.weekday()  # Monday is 0
            adjusted_date = today - timedelta(days=days_since_monday)
        else:
            # Convert the provided date string to a datetime object
            provided_date = datetime.strptime(week_start_date, "%Y-%m-%d")
            if provided_date.weekday() != 0:  # Not a Monday
                # Adjust to the most recent Monday
                days_since_monday = provided_date.weekday()
                adjusted_date = provided_date - timedelta(days=days_since_monday)
            else:
                adjusted_date = provided_date
        url = f"https://api.finra.org/data/group/otcMarket/name/weeklySummary"
        adjusted_week_start_date = adjusted_date.strftime("%Y-%m-%d")
        payload  = {"quoteValues":True,"delimiter":"|","limit":5000,"fields":["marketParticipantName","totalWeeklyShareQuantity","totalWeeklyTradeCount","lastUpdateDate"],"compareFilters":[{"fieldName":"weekStartDate","fieldValue":adjusted_week_start_date,"compareType":"EQUAL"},{"fieldName":"tierIdentifier","fieldValue":tier,"compareType":"EQUAL"},{"fieldName":"summaryTypeCode","fieldValue":"ATS_W_FIRM","compareType":"EQUAL"}]}
        async with httpx.AsyncClient(headers=self.headers) as client:

            data = await client.post(url, json=payload)

            data = data.json()
            print(data)


            totalWeekly = [i.get('totalWeeklyTradeCount') for i in data]
            totalShares = [i.get('totalWeeklyShareQuantity') for i in data]
            last_updated = [i.get('lastUpdateDate') for i in data]
            marketParticipantName = [i.get('marketParticipantName') for i in data]

            dict = { 
                'trade_count': totalWeekly,
                'shares_traded': totalShares,
                'last_updated': last_updated,
                'participant': marketParticipantName
            }

            df = pd.DataFrame(dict)

            return df
        



    async def statistics(self, week_start_date, tier:str='T1'):
        today = datetime.utcnow()
        historical_month = today.strftime("%Y-%b")
        print(f"Querying historicalMonth: {historical_month}")
        
        # Validate or calculate week_start_date
        if week_start_date is None:
            # Default to the most recent Monday
            days_since_monday = today.weekday()  # Monday is 0
            adjusted_date = today - timedelta(days=days_since_monday)
        else:
            # Convert the provided date string to a datetime object
            provided_date = datetime.strptime(week_start_date, "%Y-%m-%d")
            if provided_date.weekday() != 0:  # Not a Monday
                # Adjust to the most recent Monday
                days_since_monday = provided_date.weekday()
                adjusted_date = provided_date - timedelta(days=days_since_monday)
            else:
                adjusted_date = provided_date
        adjusted_week_start_date = adjusted_date.strftime("%Y-%m-%d")
        payload={"quoteValues":True,"delimiter":"|","limit":5000,"fields":["productTypeCode","issueSymbolIdentifier","issueName","totalWeeklyShareQuantity","totalWeeklyTradeCount","lastUpdateDate"],"compareFilters":[{"fieldName":"weekStartDate","fieldValue":adjusted_week_start_date,"compareType":"EQUAL"},{"fieldName":"tierIdentifier","fieldValue":tier,"compareType":"EQUAL"},{"fieldName":"summaryTypeCode","description":None,"fieldValue":"ATS_W_SMBL_FIRM","compareType":"EQUAL"}]}
        url = f"https://api.finra.org/data/group/otcMarket/name/weeklySummary"
        # Calculate the current month in yyyy-MMM format

        async with httpx.AsyncClient() as client:
            data = await client.post(url, headers=self.headers, json=payload)
            data = data.json()
            totalWeeklyShareQuantity = [i.get('totalWeeklyShareQuantity') for i in data]
            totalWeeklyTradeCount = [i.get('totalWeeklyTradeCount') for i in data]
            issueSymbolIdentifier = [i.get('issueSymbolIdentifier') for i in data]
            issueName = [i.get('issueName') for i in data]
            lastUpdateDate = [i.get('lastUpdateDate') for i in data]
            productTypeCode = [i.get('productTypeCode') for i in data]


            data_dict = { 
                'total_share_quantity': totalWeeklyShareQuantity,
                'total_trade_count': totalWeeklyTradeCount,
                'symbol': issueSymbolIdentifier,
                'name': issueName,
                'last_updated': lastUpdateDate,
                'product_code': productTypeCode
            }


            dataframe = pd.DataFrame(data_dict)

            return dataframe.sort_values('total_share_quantity', ascending=False)