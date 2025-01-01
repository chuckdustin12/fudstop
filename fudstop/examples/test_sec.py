import os
from dotenv import load_dotenv
import aiohttp
import pandas as pd
import asyncio
import matplotlib.pyplot as plt
load_dotenv()
from fudstop.apis.webull.webull_options import WebullOptions
from fudstop.apis.helpers import get_human_readable_string

wb = WebullOptions(user='chuck', database='charlie', host='localhost', port=5432, password='fud')

async def associate_dates_with_data(dates, datas):
    if datas is not None and dates is not None:
    # This function remains for your specific data handling if needed
        return [{**data, 'date': date} for date, data in zip(dates, datas)]

async def fetch_volume_analysis(session, option_symbol, id, underlying_ticker):
    url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={id}"
    async with session.get(url) as resp:
        if resp.status == 200:
            vol_anal = await resp.json()
            dates = vol_anal.get('dates')
            datas = vol_anal.get('datas')
            associated_data = await associate_dates_with_data(dates, datas)

            df = pd.DataFrame(associated_data)
            df['option_symbol'] = option_symbol
            components = get_human_readable_string(option_symbol)
            df['underlying_ticker'] = underlying_ticker
            df['strike'] = components.get('strike_price')
            df['call_put'] = components.get('call_put')
            df['expiry'] = components.get('expiry_date')

            return df
        else:
            print(f"Failed to fetch data for ID {id}: HTTP Status {resp.status}")
            return pd.DataFrame()

async def main(ticker, date:str='2023-12-15'):
    headers = wb.headers
    option_ids = await wb.get_option_ids(ticker)  # Adjust based on actual return structure

    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [fetch_volume_analysis(session, option_symbol, id, underlying_ticker) for option_symbol, id, underlying_ticker in option_ids]
        dfs = await asyncio.gather(*tasks)  # Run all tasks concurrently
        
    final_df = pd.concat(dfs, ignore_index=True)
    print(final_df)
    final_df.to_csv(f'volume_analysis.csv', index=False)

    # Define the visualization function
    def plot_volume_by_strike_for_date(df, target_date, dark_background=True):
        # Extract data for the specific date
        target_df = df[df['expiry'] == pd.to_datetime(target_date)]
        
        # If no data available for the target_date, inform and return without plotting
        if target_df.empty:
            print(f"No data available for the date: {target_date}")
            return

        plt.style.use('dark_background' if dark_background else 'default')
        
        # Create a plot with dark background
        fig, ax = plt.subplots(figsize=(12, 6), facecolor='black' if dark_background else 'white')
        
        # Plot buy and sell volumes
        ax.bar(target_df['strike'], target_df['buy'], label='Buy Volume', color='green', alpha=0.7)
        ax.bar(target_df['strike'], -target_df['sell'], label='Sell Volume', color='red', alpha=0.7)
        
        # Set title and axes labels with gold color and high readability
        ax.set_title(f'Buy/Sell Volume by Strike for Expiry: {pd.to_datetime(target_date).strftime("%Y-%m-%d")}', color='gold')
        ax.set_xlabel('Strike Price', color='gold')
        ax.set_ylabel('Volume', color='gold')
    
        # Tick parameters for readability
        ax.tick_params(axis='x', colors='gold')
        ax.tick_params(axis='y', colors='gold')

        # Adding grid for better readability, with a lighter color
        ax.grid(True, color='dimgray')
        
        # Show legend with readable font color
        ax.legend(facecolor='darkgray', edgecolor='gold', fontsize='large')
        
        # Show the plot
        plt.show()

    # Example usage:
    # Load the data (make sure to parse the 'date' and 'expiry' columns correctly)
    df = pd.read_csv('volume_analysis.csv', parse_dates=['date', 'expiry'])

    # Filter out rows with missing values in the volume, strike, or expiry columns if necessary
    # df = df.dropna(subset=['buy_volume', 'sell_volume', 'strike', 'expiry'])

    # Call the function with the chosen date
    plot_volume_by_strike_for_date(df, date, dark_background=True)