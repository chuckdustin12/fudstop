import os
from dotenv import load_dotenv
import requests
import pandas as pd
from .oic_models import OICOptionsMonitor
import httpx
from ..polygonio.polygon_options import PolygonOptions
db_config = {
    "host": os.environ.get('DB_HOST'), # Default to this IP if 'DB_HOST' not found in environment variables
    "port": int(os.environ.get('DB_PORT')), # Default to 5432 if 'DB_PORT' not found
    "user": os.environ.get('DB_USER'), # Default to 'postgres' if 'DB_USER' not found
    "password": os.environ.get('DB_PASSWORD'), # Use the password from environment variable or default
    "database": os.environ.get('DB_NAME') # Database name for the new jawless database
}
opts = PolygonOptions(database='fudstop3')
import time
load_dotenv()


class OICSDK:
    def __init__(self):
        self.session = requests.Session()
        self.refresh_key_payload = {
            "clientKey": f"{os.environ.get('OIC_KEY')}",
            "clientName": "OIC_test"
        }
        self.token = None

        self.initialize_token()


    def initialize_token(self):
        """
        Initialize the token by making the first request.
        """
        r = self.session.post(
            "https://ivlivefront.ivolatility.com/token/client/get",
            json=self.refresh_key_payload
        )
        if r.status_code == 200:
            self.token = r.text # Adjust the key according to actual response structure

    def build_headers(self):
        """
        Build headers using the current token.
        """
        if not self.token:
            self.initialize_token()
        return {'Authorization': f'Bearer {self.token}'}

    def refresh_token(self):
        """
        Refresh the token.
        """
        r = self.session.post(
            "https://ivlivefront.ivolatility.com/token/client/get",
            headers=self.build_headers(),
            json=self.refresh_key_payload
        )
        if r.status_code == 200:
            self.token = r
        return self.token
    def get_option_id(self, ticker):
        """
        Gets the option ID using the lookup endpoint
        
        """
        url = f"https://private-authorization.ivolatility.com/lookup/?sortField=SYMBOL&symbol={ticker}&region=1&matchingType=EXACTLY&pageSize=10&page=0"

        r = self.session.get(url, headers=self.build_headers()).json()
        pages = r['page']
        id = pages[0]['stockId']
        return id
    def most_active_options(self):
        """
        Gets the most active options from the Options Industry Council
        
        """
        url=f"https://private-authorization.ivolatility.com/favorites/instruments/most-active"
        data = self.session.get(url, headers=self.build_headers()).json()


        print(data)

        df = pd.DataFrame(data)
        return df

    async def get_price(self, ticker):
        try:
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&limit=1&apiKey={self.api_key}"
            print(url)
            async with httpx.AsyncClient() as client:
                r = await client.get(url)
                if r.status_code == 200:
                    r = r.json()
                    results = r['results'] if 'results' in r else None
                    if results is not None:
                        session = [i.get('session') for i in results]
                        price = [i.get('close') for i in session]
                        print(price)
        except Exception as e:
            print(e)
    async def options_monitor(self, ticker):
        await opts.connect()
        stock_id = self.get_option_id(ticker)
        stock_price = self.get_price(ticker)
        print(stock_price)

        url = f"https://private-authorization.ivolatility.com/options-monitor/listOptionDataRow?stockId={stock_id}&center={stock_price}&regionId=1&strikesN=75&columns=alpha&columns=ask&columns=asksize&columns=asktime&columns=bid&columns=bidsize&columns=bidtime&columns=change&columns=changepercent&columns=delta&columns=theoprice&columns=gamma&columns=ivint&columns=ivask&columns=ivbid&columns=mean&columns=openinterest_eod&columns=optionsymbol&columns=volume&columns=paramvolapercent_eod&columns=alpha_eod&columns=ask_eod&columns=bid_eod&columns=delta_eod&columns=theoprice_eod&columns=gamma_eod&columns=ivint_eod&columns=mean_eod&columns=rho_eod&columns=theta_eod&columns=vega_eod&columns=changepercent_eod&columns=change_eod&columns=volume_eod&columns=quotetime&columns=rho&columns=strike&columns=style&columns=theta&columns=vega&columns=expirationdate&columns=forwardprice&columns=forwardprice_eod&columns=days&columns=days_eod&columns=iv&columns=iv_eod&rtMode=RT&userId=9999999&uuid=null"
        print(url)

        # Add your logic here to process the URL
        # For example, making an HTTP request

        r = requests.get(url, headers=self.build_headers()).json()


        data = OICOptionsMonitor(r)
        try:
            df = data.as_dataframe
            await opts.batch_insert_dataframe(df, table_name='monitor', unique_columns='insertion_timestamp', batch_size=1)
        except Exception as e:
            print(e)
    
      
        return data
    

    def iv_monitor(self):
        url = f"https://.ivolatility.com/ivx-monitor"
        r = requests.get(url, headers=self.build_headers()).json()
        return r




