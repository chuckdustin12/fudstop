import os

from dotenv import load_dotenv
load_dotenv()
import pandas as pd
from fudstop.apis.polygonio.polygon_options import PolygonOptions
from datetime import datetime
from discord_webhook import AsyncDiscordWebhook
from fudstop.apis.gexbot.gexbot import GEXBot
opts = PolygonOptions()
gex = GEXBot()
import asyncio




# Initialize previous values to None

# Global dictionary to maintain previous values for each ticker
prev_values = {}

async def main(ticker):
    global prev_values  # Reference the global prev_values dictionary

    # Initialize prev_values for the ticker if not already done
    if ticker not in prev_values:
        prev_values[ticker] = {
            "major_pos_vol": None,
            "major_neg_vol": None,
            "major_pos_oi": None,
            "major_neg_oi": None,
            "zero_gamma": None,
        }

    # Get the latest data
    df = await gex.major_levels(ticker)
    df = df.transpose()

    # Extract current values
    current_values = {
        "major_pos_vol": df['major_pos_vol'].iloc[0],
        "major_neg_vol": df['major_neg_vol'].iloc[0],
        "major_pos_oi": df['major_pos_oi'].iloc[0],
        "major_neg_oi": df['major_neg_oi'].iloc[0],
        "zero_gamma": df['zero_gamma'].iloc[0]
    }

    # Check for any changes and build the message
    message_content = f"> GEX CHANGE: \n"
    change_detected = False
    for key in current_values:
        if current_values[key] != prev_values[ticker].get(key):
            change_detected = True
            prev_value = prev_values[ticker].get(key, 'N/A')
            current_value = current_values[key]
            message_content += f"> {key.replace('_', ' ').title()}: Prev: **${round(float(prev_value),2) if prev_value is not None else prev_value}** > Now: **${round(float(current_value),2)}**\n"

    # Send the message to Discord only if there's a change
    if change_detected:
        webhook = AsyncDiscordWebhook(
            "https://discord.com/api/webhooks/1126626151750709368/NHco29Iobl8BbwiQfZ2iTZTi6pi16Lpe1b0_4tVN1f-BbD7MzpI3IlaW50jJzgNJvaqg",
            content=message_content
        )
        await webhook.execute()

        # Update the previous values
        prev_values[ticker] = current_values.copy()

async def run_main():
    ticker_list = ['SPY', 'SPX']

    while True:
        start_time = datetime.now()

        # Create and run tasks for each ticker
        tasks = [main(ticker) for ticker in ticker_list]
        await asyncio.gather(*tasks)

        # Calculate elapsed time and wait if needed
        elapsed_time = (datetime.now() - start_time).total_seconds()
        if elapsed_time < 60:
            await asyncio.sleep(60 - elapsed_time)

asyncio.run(run_main())