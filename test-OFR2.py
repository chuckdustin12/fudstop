import asyncio
import aiohttp
from fudstop4.apis.helpers import format_large_numbers_in_dataframe2
import pandas as pd
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
opts = PolygonOptions()
# List of datasets to process
datasets = ['fpf', 'tff', 'scoos', 'ficc']
import numpy as np

import asyncio
import aiohttp
import pandas as pd

# List of datasets to process
datasets = ['nypd', 'mmf', 'repo', 'fnyr', 'tyld']

async def fetch_json(session, url):
    """
    Asynchronously fetch JSON data from a URL.
    
    Args:
        session (aiohttp.ClientSession): The aiohttp session.
        url (str): The URL to fetch.
    
    Returns:
        dict or list: The JSON response.
    """
    async with session.get(url) as response:
        return await response.json()

async def fetch_mnemonics(session, dataset):
    """
    Fetch the mnemonics for a given dataset.
    
    Args:
        session (aiohttp.ClientSession): The aiohttp session.
        dataset (str): The dataset identifier.
    
    Returns:
        list: A list of mnemonic objects, each containing keys like 'mnemonic' and 'series_name'.
    """
    url = f"https://data.financialresearch.gov/v1/metadata/mnemonics?dataset={dataset}"
    data = await fetch_json(session, url)
    return data

async def fetch_mnemonic_details(session, mnemonic, series_name=None):
    """
    Fetch the details for a given mnemonic.
    
    Args:
        session (aiohttp.ClientSession): The aiohttp session.
        mnemonic (str): The mnemonic identifier.
        series_name (str, optional): The series name from the mnemonics endpoint.
    
    Returns:
        dict: A dictionary containing details for the mnemonic.
    """
    url = f"https://data.financialresearch.gov/v1/metadata/query?mnemonic={mnemonic}"
    data = await fetch_json(session, url)
    desc = data.get('description') or {}
    schedule = data.get('schedule')
    result = {
        'mnemonic': data.get('mnemonic'),
        'series_name': series_name,
        'notes': desc.get('notes'),
        'start_date': schedule.get('start_date'),
        'last_update': schedule.get('last_update'),
        'observation_period': schedule.get('observation_period'),
        'observation_frequency': schedule.get('observation_frequency')
    }
    return result

async def process_dataset(session, dataset):
    """
    Process a single dataset: fetch mnemonics and then their details concurrently.
    
    Args:
        session (aiohttp.ClientSession): The aiohttp session.
        dataset (str): The dataset identifier.
    
    Returns:
        list: A list of records (dictionaries) with details for each mnemonic.
    """
    results = []
    mnemonic_items = await fetch_mnemonics(session, dataset)
    
    # Create tasks for fetching details for each mnemonic concurrently
    tasks = []
    for item in mnemonic_items:
        mnemonic = item.get('mnemonic')
        # Try to get the series name from either 'series_name' or 'seriesName'
        series_name = item.get('series_name') or item.get('seriesName')
        tasks.append(asyncio.create_task(fetch_mnemonic_details(session, mnemonic, series_name)))
    
    details = await asyncio.gather(*tasks)
    
    # Tag each detail with its dataset and add to the results list
    for detail in details:
        detail['dataset'] = dataset
        results.append(detail)
    
    return results

async def main():
    """
    Main async function that processes all datasets concurrently,
    flattens the results, builds a Pandas DataFrame, and prints it.
    """
    await opts.connect()
    async with aiohttp.ClientSession() as session:
        tasks = [process_dataset(session, dataset) for dataset in datasets]
        results_nested = await asyncio.gather(*tasks)
        
        # Flatten the list of lists into a single list of records
        all_results = [record for sublist in results_nested for record in sublist]
        
        # Create and output the final dataframe
        df = pd.DataFrame(all_results)
        print(df)
        await opts.batch_upsert_dataframe(df, table_name='ofr_stf_monitor', unique_columns=['mnemonic'])
        return df

import requests


async def main2():
    # Connect to your database (this example assumes opts has an async connect() method)
    await opts.connect()
    
    # Your query to fetch necessary fields from the database
    query = "SELECT mnemonic, series_name, notes FROM ofr_stf_monitor"
    results = await opts.fetch(query)

    # This list will hold all generated endpoint functions as string code
    endpoints_code = []
    
    for result in results:
        mnemonic = result.get('mnemonic')
        series_name = result.get('series_name')
        notes = result.get('notes')
        
        # Generate a valid route name by replacing unwanted characters.
        # You may want to extend or adjust these replacements based on your data.
        route_name = series_name.replace(' ', '_') \
                               .replace(':', '') \
                               .replace('(', '') \
                               .replace(')', '') \
                               .replace('&', '_') \
                               .replace('<=', 'lte') \
                               .replace('<', 'lt') \
                               .replace('>=', 'gte') \
                               .replace('>', 'gt') \
                               .replace('.', '') \
                               .replace('-', '') \
                               .replace('position', 'pos') \
                               .replace('futures', 'futes') \
                               .replace('dollars', 'dlrs') \
                               .replace('s_p_500', 'sp500') \
                               .replace('strategy', 'strat') \
                               .replace('average', 'avg') \
                               .replace('percent', 'pct') \
                               .replace('quarterly', 'qrtly') \
                               .replace('qualifying', '') \
                               .replace("'s", 's') \
                               .replace('portfolio', 'port') \
                               .replace('hedge_funds', 'hedgefunds') \
                               .replace('+', 'plus') \
                               .replace('tri_party', 'triparty') \
                               .replace('excluding', 'excl') \
                               .replace('_hedge', 'hedge') \
                               .lower()
        
        # Create an endpoint function as a string using an f-string.
        # Note: We enclose string values (like mnemonic, series_name, and notes)
        # in quotes so that they become literal values in the generated code.
        endpoint_function = f'''
@dealerrouter.route('/{route_name}')
async def {route_name}():
    """
    Endpoint for series: {series_name}
    Mnemonic: {mnemonic}
    Notes: {notes}
    """
    mnemonic = "{mnemonic}"
    # Retrieve time series data using the mnemonic
    r = requests.get(f"https://data.financialresearch.gov/v1/series/timeseries?mnemonic={mnemonic}")
    data = r.json()
    
    # Create a DataFrame from the returned data
    df = pd.DataFrame(data, columns=['date', 'value'])
    # Reverse the dataframe order if needed
    df = df[::-1]
    
    # Add metadata columns to the DataFrame
    df['name'] = "{series_name}"
    df['notes'] = "{notes}"
    
    # Format the dataframe (e.g., large number formatting)
    df = format_large_numbers_in_dataframe2(df)
    
    # Replace NaN values with None and convert to list of dictionaries
    json_data = df.replace({{np.nan: None}}).to_dict('records')
    return json_data
'''
        endpoints_code.append(endpoint_function)

    # Write all generated endpoints to a Python file
    with open("generated_endpoints.py", "w") as f:
        # Write necessary imports at the top of the file
        f.write("from fastapi import APIRouter\n")
        f.write("import requests\n")
        f.write("import pandas as pd\n")
        f.write("import numpy as np\n\n")
        f.write("from fudstop4.apis.helpers import format_large_number_in_dataframe2")
        f.write("dealerrouter = APIRouter()\n\n")
        f.write("def format_large_numbers_in_dataframe2(df):\n")
        f.write("    \"\"\"\n")
        f.write("    Placeholder function for formatting large numbers in the dataframe.\n")
        f.write("    Implement your formatting logic here.\n")
        f.write("    \"\"\"\n")
        f.write("    return df\n\n")
        
        # Write each generated endpoint function to the file
        for endpoint in endpoints_code:
            f.write(endpoint)
            f.write("\n")
    
    print("Endpoints have been saved to generated_endpoints.py")

# Run the asynchronous main2 function
asyncio.run(main2())