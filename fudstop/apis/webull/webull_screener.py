import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import requests
from .webull_options import WebullOptionsData
from typing import List
from .screener_models import ScreenerRule, TickerInfo
load_dotenv()




class WebulScreener:
    def __init__(self):
        self.pool = None
        self.as_dataframe = None
        self.session = requests.session()
        self.account_id=os.environ.get('WEBULL_ACCOUNT_ID')
        self.api_key = os.environ.get('YOUR_POLYGON_KEY')
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        self.headers = {
        "Access_token": os.environ.get('webull_access_token'),
        "Accept": "*/*",
        "App": "global",
        "App-Group": "broker",
        "Appid": "wb_web_app",
        "Content-Type": "application/json",
        "Device-Type": "Web",
        "Did": os.environ.get('DID'),
        "Hl": "en",
        "Locale": "eng",
        "Os": "web",
        "Osv": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Ph": "Windows Chrome",
        "Platform": "web",
        "Referer": "https://app.webull.com/",
        "Reqid": os.environ.get('REQ_ID'),
        "Sec-Ch-Ua": "\"Chromium\";v=\"118\", \"Google Chrome\";v=\"118\", \"Not=A?Brand\";v=\"99\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "T_time": "1698276695206",
        "Tz": "America/Los_Angeles",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Ver": "4.0.6",
        "X-S": os.environ.get('YOUR_X-S'),
        "X-Sv": "xodp2vg9"
    }

    def get_option_data_for_id(self, id, symbol):
        print(f"Starting processing for ticker: {id}")
        dataframes = []  # Initialize a list to collect DataFrames
  

        print(f"Processing batch ID: {id} for ticker: {id}")
        url = f"https://quotes-gw.webullfintech.com/api/quote/option/quotes/queryBatch?derivativeIds={id}"

        data = self.session.get(url, headers=self.headers).json()
        if not data:  # If data is empty or None, break the loop
            print(f"No more data for ticker: {id}. Moving to next.")
          
        wb_data = WebullOptionsData(data)
        if self.as_dataframe is not None:
            df = wb_data.as_dataframe
            df['ticker'] = id
            df['symbol'] = symbol
            df = df.rename(columns={'open_interest_change': 'oi_change'})
            return df
    def query(self, 
            ask_gte=None, ask_lte=None,
            bid_gte=None, bid_lte=None,
            changeRatio_gte=None, changeRatio_lte=None,
            close_gte=None, close_lte=None,
            delta_gte=None, delta_lte=None,
            direction=None,
            expireDate_gte=None, expireDate_lte=None,
            gamma_gte=None, gamma_lte=None,
            implVol_gte=None, implVol_lte=None,
            openInterest_gte=None, openInterest_lte=None,
            rho_gte=None, rho_lte=None,
            theta_gte=None, theta_lte=None,
            vega_gte=None, vega_lte=None,
            volume_gte=None, volume_lte=None):

        url = "https://quotes-gw.webullfintech.com/api/wlas/option/screener/query"
        # Initialize an empty dictionary for the filter
        filter_dict = {}

        # Helper function to add filter criteria
        def add_filter_criteria(name, gte, lte):
            if gte is not None and lte is not None:
                filter_dict[name] = f"gte={gte}&lte={lte}"

        # Add criteria to the filter dictionary if they are provided
        add_filter_criteria("options.screener.rule.ask", ask_gte, ask_lte)
        add_filter_criteria("options.screener.rule.bid", bid_gte, bid_lte)
        add_filter_criteria("options.screener.rule.changeRatio", changeRatio_gte, changeRatio_lte)
        add_filter_criteria("options.screener.rule.close", close_gte, close_lte)
        add_filter_criteria("options.screener.rule.delta", delta_gte, delta_lte)
        if direction:
            filter_dict["options.screener.rule.direction"] = direction
        add_filter_criteria("options.screener.rule.expireDate", expireDate_gte, expireDate_lte)
        add_filter_criteria("options.screener.rule.gamma", gamma_gte, gamma_lte)
        add_filter_criteria("options.screener.rule.implVol", implVol_gte, implVol_lte)
        add_filter_criteria("options.screener.rule.openInterest", openInterest_gte, openInterest_lte)
        add_filter_criteria("options.screener.rule.rho", rho_gte, rho_lte)
        add_filter_criteria("options.screener.rule.theta", theta_gte, theta_lte)
        add_filter_criteria("options.screener.rule.vega", vega_gte, vega_lte)
        add_filter_criteria("options.screener.rule.volume", volume_gte, volume_lte)

        # Build the payload
        payload = {
            "filter": filter_dict,
            "page": {'fetchSize': 25}
        }

        
        data= self.session.post(url, headers=self.headers, json=payload).json()
        datas = data.get('datas')

        belongTickerId = [i.get('belongTickerId') for i in datas]
        derivativeId = [i.get('derivativeId') for i in datas]
        derivative = [i.get('derivative') for i in datas]
        values = [i.get('values') for i in datas]

            


        derivative = TickerInfo(derivative)
    
        data_dict = { 

            'symbol': derivative.unSymbol,
            'strike': derivative.strikePrice,
            'call_put': derivative.direction,
            'expiry': derivative.expireDate,
            'id': derivativeId
       
        }

    

        return data_dict


import disnake
class ScreenerSelect(disnake.ui.Select):
    def __init__(self, data_dict):
        self.data_dict = data_dict

        options = []
        # Ensure all lists are of the same length
        num_items = len(data_dict['id'])
        if all(len(data_dict[key]) == num_items for key in ['symbol', 'strike', 'expiry', 'call_put']):
            for i in range(num_items):
                # Construct the label and value for each option
                label = f"{data_dict['symbol'][i]} | Strike: {data_dict['strike'][i]} | Expiry: {data_dict['expiry'][i]} | {data_dict['call_put'][i]}"
                value = str(data_dict['id'][i])

                options.append(disnake.SelectOption(label=label, value=value))
        else:
            print("Data lists are not of equal length")

        super().__init__(
            placeholder="> Results >",
            min_values=1,
            max_values=len(options),
            custom_id='optionscreener',
            options=options
        )
        super().__init__(
            placeholder="> Results >",
            min_values=1,
            max_values=len(data_dict),
            custom_id='optionscreener',
            options=options
        )




    async def callback(self, inter:disnake.AppCmdInter):
        await inter.response.defer()
        if self.values[0]:

            url = requests.get(f"https://quotes-gw.webullfintech.com/api/quote/option/quotes/queryBatch?derivativeIds={','.join(self._selected_values)}").json()
            print(url)

            data = WebullOptionsData(url).data_dict

            description = self.format_data_for_embed(data)
            embed = disnake.Embed(title=f"Results for Option:", description=f"> ***{description}***")
            view = disnake.ui.View()
            await inter.edit_original_message(embed=embed)

    def format_data_for_embed(self, data):
        # Format each key-value pair in data
        formatted_data = []
        for key, values in data.items():
            formatted_values = ', '.join(str(value) for value in values)
            formatted_data.append(f"**{key}:** {formatted_values}")

        # Join all formatted data into a single string
        return '\n'.join(formatted_data)