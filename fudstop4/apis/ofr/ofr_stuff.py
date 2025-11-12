import asyncio
from fudstop4.apis.polygonio.async_polygon_sdk import Polygon

from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions()
poly = Polygon()

import requests
import pandas as pd
import json

import asyncpg
from fudstop4.apis.ofr.ofr_sdk import OFR, TYLD_OFR,MMF_OFR,FNYR_OFR,NYPD_OFR,REPO_OFR

import aiohttp

ofr = OFR()
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "chuck",
    "password": "fud",
    "database": "market_data"
}
# Define all mnemonics
mnemonics = {
    "TYLD_OFR": TYLD_OFR,
    "MMF_OFR": MMF_OFR,
    "FNYR_OFR": FNYR_OFR,
    "NYPD_OFR": NYPD_OFR,
    "REPO_OFR": REPO_OFR,
}
# Load URLs from the CSV
mnemonic_df = pd.read_csv("combined_mnemonics.csv")
urls_with_names = mnemonic_df[["Name", "URL"]].to_dict(orient="records")
async def fetch_data(session, url):
    """Fetch JSON data from a URL."""
    async with session.get(url) as response:
        return await response.json()  # Assuming the URL returns JSON-formatted data


urls_with_names = mnemonic_df[["Name", "URL"]].to_dict(orient="records")
async def drop_tables(urls_with_names):
    """Drop tables based on extracted names from URLs."""
    async with asyncpg.create_pool(**DB_CONFIG) as pool:
        async with pool.acquire() as conn:
            try:
                for item in urls_with_names:
                    url = item["URL"]
                    # Extract the full mnemonic and format the table name
                    mnemonic = url.split("mnemonic=")[1].split("&")[0]
                    table_name = mnemonic.lower().replace("-", "_")  # Format table name
                    print(f"Dropping table: {table_name}")
                    # Drop table if it exists
                    await conn.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
                print("All specified tables have been dropped.")
            except Exception as e:
                print(f"Error dropping tables: {e}")

async def fetch_data(session, url):
    """Fetch JSON data from a URL."""
    async with session.get(url) as response:
        return await response.json()  # Assuming the URL returns JSON-formatted data

async def fetch_all_data(urls_with_names):
    """Fetch data from all URLs and include full mnemonic and name."""
    combined_data = []
    async with aiohttp.ClientSession() as session:
        for item in urls_with_names:
            url = item["URL"]
            readable_name = item["Name"]  # Extract the human-readable name
            try:
                # Extract the full mnemonic from the URL
                mnemonic = url.split("mnemonic=")[1].split("&")[0]
                
                # Format the table name
                table_name = mnemonic.lower().replace("-", "_")  # Convert to lowercase and replace "-" with "_"

                # Fetch data from the URL
                data = await fetch_data(session, url)

                # Convert the fetched data into a DataFrame
                df = pd.DataFrame(data, columns=["Date", "Value"])
                df["Mnemonic"] = table_name  # Add formatted mnemonic name
                df["Readable_Name"] = readable_name  # Add full human-readable name
                df.columns = df.columns.str.lower()  # Convert columns to lowercase

                # Batch insert into the database
                await db.batch_upsert_dataframe(
                    df, table_name=table_name, unique_columns=["date"]
                )
                
                # Store the data in combined_data for further use or exporting
                combined_data.append(df)
            except Exception as e:
                print(f"Failed to fetch data for URL {url}: {e}")
    return pd.concat(combined_data, ignore_index=True)


# Main asyncio function to run the fetching process
async def main():
    await db.connect()  # Connect to the database
    
    # Drop existing tables
    await drop_tables(urls_with_names)

    # Fetch and process data
    combined_df = await fetch_all_data(urls_with_names)
    combined_df.to_csv("combined_data_with_full_mnemonics.csv", index=False)  # Save combined data to CSV
    await db.batch_upsert_dataframe(combined_df, table_name='all_ofr', unique_columns=['date', 'readable_name', 'mnemonic', 'value'])

# Run the async function
asyncio.run(main())