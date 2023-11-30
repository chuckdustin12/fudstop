import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from asyncpg import create_pool
import requests
import matplotlib.pyplot as plt
from decimal import Decimal
import matplotlib.dates as mdates
import aiohttp
import asyncio
from .treasury_models import AvgInterestRates
from asyncpg.exceptions import UndefinedTableError
from datetime import datetime, timedelta
import pandas as pd
from datetime import datetime

from database_.database_manager import DatabaseManager



db_config = {
    "host": os.environ.get('DB_HOST'), # Default to this IP if 'DB_HOST' not found in environment variables
    "port": int(os.environ.get('DB_PORT')), # Default to 5432 if 'DB_PORT' not found
    "user": os.environ.get('DB_USER'),
    "password": os.environ.get('DB_PASSWORD'), # Use the password from environment variable or default
    "database": os.environ.get('DB_NAME') # Database name for the new jawless database
}

db_manager = DatabaseManager(**db_config)


# Determine the table name based on the endpoint
endpoint_to_table = {
    "/v1/accounting/od/securities_sales": "Sales",
    "/v1/accounting/od/securities_sales_term": "Sales by Term",
    "/v1/accounting/od/securities_transfers": "Transfers of Marketable Securities",
    "/v1/accounting/od/securities_conversions": "Conversions of Paper Savings Bonds",
    "/v1/accounting/od/securities_redemptions": "Redemptions",
    "/v1/accounting/od/securities_outstanding": "Outstanding",
    "/v1/accounting/od/securities_c_of_i": "Certificates of Indebtedness",
    "/v1/accounting/od/securities_accounts": "Accounts",
}

class Treasury:
    def __init__(self, host:str=os.environ.get('DB_HOST'), port:str=os.environ.get('DB_PORT'), user:str=os.environ.get('DB_USER'), password:str=os.environ.get('DB_USER'), database:str=os.environ.get('DB_MNAME')):
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')


        self.base_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
        self.conn = None
        self.pool = None
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        self.chat_memory = []  # In-memory list to store chat messages


        self.endpoint_to_table = {
            "/v1/accounting/od/securities_sales": "td_sales",
            "/v1/accounting/od/securities_sales_term": "td_sales_term",
            "/v1/accounting/od/securities_transfers": "td_transfers",
            "/v1/accounting/od/securities_conversions": "td_conversions",
            "/v1/accounting/od/securities_redemptions": "td_redemptions",
            "/v1/accounting/od/securities_outstanding": "td_outstanding",
            "/v1/accounting/od/securities_c_of_i": "td_indebtedness",
            "/v1/accounting/od/securities_accounts": "td_accounts",
            '/v2/accounting/od/gold_reserve': "fed_gold_vault",
            '/v1/accounting/tb/pdo2_offerings_marketable_securities_other_regular_weekly_treasury_bills': "marketable_securities",
        }



    async def connect(self):
        self.pool = await create_pool(
            **db_config, min_size=1, max_size=10
        )

        return self.pool

    async def disconnect(self):
        await self.pool.close()
    async def insert_treasury_data(self, table_name, data):
        try:
            # Convert 'record_date' from string to date object if it exists in data
            if 'record_date' in data:
                data['record_date'] = datetime.strptime(data['record_date'], '%Y-%m-%d').date()

            # Corrected the keys for 'auction_date' and 'issue_date'
            if 'auction_date' in data:
                data['auction_date'] = datetime.strptime(data['auction_date'], '%Y-%m-%d').date()

            if 'issue_date' in data:
                data['issue_date'] = datetime.strptime(data['issue_date'], '%Y-%m-%d').date()

            # Convert known fields that should be integers
            # Convert known fields that should be integers
            integer_fields = [
                'securities_sold_cnt', 'src_line_nbr', 'record_fiscal_year', 'record_fiscal_quarter',
                'record_calendar_year', 'record_calendar_quarter', 'record_calendar_month', 'record_calendar_day',
                'securities_redeemed_cnt', 'from_legacy_system_cnt', 'from_commercial_book_entry_cnt',
                'total_incoming_transfers_cnt', 'unrestrict_primary_accts_cnt', 'funded_accts_cnt','security_term_nbr','paper_sb_conversions_cnt'
            ]

            float_fields = ['securities_sold_amt', 'securities_redeemed_amt']

            decimal_fields = [
                'outstanding_cert_indebt_amt',  # Add other decimal fields if needed
            ]

            for field in integer_fields:
                if field in data:
                    data[field] = int(data[field]) if data[field] != 'null' else None

            for field in float_fields:
                if field in data:
                    data[field] = float(data[field]) if data[field] != 'null' else None

            for field in decimal_fields:
                if field in data:
                    data[field] = Decimal(data[field]) if data[field] != 'null' else None
            async with self.pool.acquire() as conn:
                # Construct the insert query dynamically based on the data dictionary
                columns = ', '.join(data.keys())
                placeholders = ', '.join([f"${i+1}" for i in range(len(data))])

                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"

                print(f"Debug - SQL Query: {insert_query}")
                print(f"Debug - Data Values: {list(data.values())}")

                await conn.execute(insert_query, *data.values())

            print(f"Data inserted into {table_name} successfully.")

        except Exception as e:
            print(f"Error inserting data into {table_name}: {e}")
            print(f"ATTEMPTING to create table...")
            await db_manager.create_table(data, table_name='fed_gold', unique_column='insertion_timestamp')

    async def fetch_gold_data(self):
        await self.connect()
        async with self.pool.acquire() as conn:
            query = "SELECT record_date, book_value_amt, fine_troy_ounce_qty FROM fed_gold ORDER BY record_date ASC;"
            try:
                rows = await conn.fetch(query)

                
                return rows
            except UndefinedTableError:
                print(f'Tables doesnt exist!')
          
    async def plot_gold_data(self):
        # Step 1: Fetch data
        data = await self.fetch_gold_data()

        # Step 2: Convert to DataFrame
        df = pd.DataFrame(data, columns=['record_date', 'book_value_amt', 'fine_troy_ounce_qty'])
        
        # Convert 'record_date' to datetime format for better x-axis formatting
        df = df.sort_values(by='record_date')

        # Plot
        fig, ax1 = plt.subplots(figsize=(14, 8))

        # Plotting fine_troy_ounce_qty
        ax1.plot(df['record_date'], df['fine_troy_ounce_qty'], 'b-', marker='o', label='Fine Troy Ounce Quantity')
        ax1.set_xlabel('Record Date')
        ax1.set_ylabel('Fine Troy Ounce Quantity', color='b')
        ax1.tick_params('y', colors='b')



        # Title and grid
        plt.title('Fine Troy Ounce Quantity vs Book Value Amount Over Time')
        ax1.grid(True)

        # Legend
        ax1.legend(loc='upper left', bbox_to_anchor=(0.0,1), fontsize=10)


        plt.show()


        
    def data_act_compliance(self):
        url=self.base_url+f"/v2/debt/tror/data_act_compliance?filter=record_date:gte:2018-07-01,record_date:lte:{self.today}&sort=-record_date,agency_nm,agency_bureau_indicator,bureau_nm"
        r = requests.get(url).json()
        data = r['data']
        df = pd.DataFrame(data)

        return df



    async def query_treasury(self, endpoint):
        await self.connect()
        

        url = self.base_url + endpoint + f"?sort=-record_date" #f"?filter=record_date:gte:{two_years_ago_str}"
        
        print(url)
        
        table_name = self.endpoint_to_table.get(endpoint, None)
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as resp:
                    r = await resp.json()
                    data = r['data']
                    print(data)

                    if table_name:
                        for record in data:
                            await self.insert_treasury_data(table_name, record)
                    else:
                        print(f"Table name not found for endpoint: {endpoint}")

            except aiohttp.ClientError as e:
                print(f"Error fetching data from {url}: {e}")
            except Exception as e:
                print(f"Error: {e}")



    def avg_interest_rates(self):
        """Gets avg. interest rates for US treasury"""

        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates?sort=-record_date").json()
        data = r['data']


        return AvgInterestRates(data)


