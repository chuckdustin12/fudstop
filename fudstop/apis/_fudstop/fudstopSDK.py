from imps import *
import os
from dotenv import load_dotenv
load_dotenv()

class fudSTOPSDK:
    def __init__(self):
        self.rsi_hooks = { 
            'day': os.environ.get('DAY_RSI'),
            'week': os.environ.get('WEEK_RSI'),
            'hour': os.environ.get('HOUR_RSI'),
            'month': os.environ.get('MONTH_RSI'),
            'minute': os.environ.get('MINUTE_RSI'),
        }



    async def send_hook(webhook):
        print(webhook)