import os
from dotenv import load_dotenv
import requests
import pandas as pd
from .oic_models import OICOptionsMonitor
load_dotenv()


class OICSDK:
    def __init__(self):
        self.session = requests.Session()
        self.refresh_key_payload = {
            "clientKey": f"{os.environ.get('YOUR_OIC_KEY')}",
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

    def get_price(self, ticker):
        r = requests.get(f"https://api.polygon.io/v2/last/nbbo/{ticker}?apiKey={os.environ.get('YOUR_POLYGON_KEY')}").json()
        results = r['results'] if 'results' in r else None
        if results is not None:
            price = results['P']
            print(price)
            return price

    def options_monitor(self, ticker):
        stock_id = self.get_option_id(ticker)
        stock_price = self.get_price(ticker)
        print(stock_price)

        url = f"https://private-authorization.ivolatility.com/options-monitor/listOptionDataRow?stockId={stock_id}&center={stock_price}&regionId=1&strikesN=75&columns=alpha&columns=ask&columns=asksize&columns=asktime&columns=bid&columns=bidsize&columns=bidtime&columns=change&columns=changepercent&columns=delta&columns=theoprice&columns=gamma&columns=ivint&columns=ivask&columns=ivbid&columns=mean&columns=openinterest_eod&columns=optionsymbol&columns=volume&columns=paramvolapercent_eod&columns=alpha_eod&columns=ask_eod&columns=bid_eod&columns=delta_eod&columns=theoprice_eod&columns=gamma_eod&columns=ivint_eod&columns=mean_eod&columns=rho_eod&columns=theta_eod&columns=vega_eod&columns=changepercent_eod&columns=change_eod&columns=volume_eod&columns=quotetime&columns=rho&columns=strike&columns=style&columns=theta&columns=vega&columns=expirationdate&columns=forwardprice&columns=forwardprice_eod&columns=days&columns=days_eod&columns=iv&columns=iv_eod&rtMode=RT&userId=9999999&uuid=null"
        print(url)

        # Add your logic here to process the URL
        # For example, making an HTTP request

        r = requests.get(url, headers=self.build_headers()).json()


        data = OICOptionsMonitor(r)


        return data
    

    def iv_monitor(self):
        url = f"https://.ivolatility.com/ivx-monitor"
        r = requests.get(url, headers=self.build_headers()).json()
        return r




