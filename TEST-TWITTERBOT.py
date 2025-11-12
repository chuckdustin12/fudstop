import os
import requests
from requests_oauthlib import OAuth1
from dotenv import load_dotenv
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from fudstop4.apis.webull.webull_options.webull_options import WebullOptions
from fudstop4.apis.helpers import generate_webull_headers
wbopts = WebullOptions()
poly = PolygonOptions()
import asyncio
import pandas as pd
load_dotenv()

API_KEY = os.environ.get("X_API_KEY")
API_SECRET = os.environ.get("X_API_SECRET")
ACCESS_TOKEN = os.environ.get("X_ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("X_ACCESS_SECRET")

TWITTER_POST_URL = "https://api.twitter.com/2/tweets"

def post_tweet(text: str):
    if not all([API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET]):
        raise EnvironmentError("Missing Twitter API credentials.")

    auth = OAuth1(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    payload = {
        "text": text
    }

    response = requests.post(TWITTER_POST_URL, auth=auth, json=payload)

    if response.status_code == 201:
        print("✅ Tweet posted successfully.")
    else:
        print(f"❌ Failed to post tweet: {response.status_code} - {response.text}")

    return response.json()



async def main():
    x = await wbopts.multi_options(ticker='SPY', headers=generate_webull_headers())


    df = x.as_dataframe

    call_df = df[df['call_put'] == 'call']
    put_df = df[df['call_put'] == 'put']

    call_volume  = call_df['volume'].sum()
    put_volume   = put_df['volume'].sum()

    post_tweet(f"📣 SPY VOLUME ALERT: SPY Call Volume: {call_volume}\nSPY Put Volume: {put_volume}")



asyncio.run(main())