from apis.ofr.ofr_sdk import OFR
from apis.ofr.ofr_list_sets import REPO_OFR,NYPD_OFR,TYLD_OFR,MMF_OFR,FNYR_OFR

import asyncio
import aiohttp

import pandas as pd



sdk = OFR()
connection_string = None
async def main():
    await sdk.connect()
    mnemonics = await sdk.query_ofr_api(sdk.endpoints.get('mnemonics'))
    for mnemonic in mnemonics:
        series_data = await sdk.construct_series_url(mnemonic)
        async with aiohttp.ClientSession() as session:
            async with session.get(series_data) as resp:
                if str(mnemonic).startswith('REPO'):


                    name = REPO_OFR[mnemonic]
                    print(name)

                    data = await resp.json()
                    print(data)
                    df = pd.DataFrame(data, columns=['date', 'value'])

                    df['name'] = name
                    df['mnemonic'] = mnemonic
                    df['value'] = df['value'].apply(lambda x: format(float(x), '.2f') if x is not None else x)
                    df = df[::-1]
                    print(df)
                    if connection_string is not None:
                        await sdk.insert_ofr_data(df)
                elif str(mnemonic).startswith('TYLD'):
                    name = TYLD_OFR[mnemonic]
                    print(name)
                    data = await resp.json()
                    print(data)
                    df = pd.DataFrame(data, columns=['date', 'value'])

                    df['name'] = name
                    df['mnemonic'] = mnemonic
                    df['value'] = df['value'].apply(lambda x: format(float(x), '.2f') if x is not None else x)
                    df = df[::-1]
                    print(df)
                    if connection_string is not None:
                        await sdk.insert_ofr_data(df)
                elif str(mnemonic).startswith('MMF'):
                    name = MMF_OFR[mnemonic]
                    print(name)
                    data = await resp.json()
                    print(data)
                    df = pd.DataFrame(data, columns=['date', 'value'])

                    df['name'] = name
                    df['mnemonic'] = mnemonic
                    df['value'] = df['value'].apply(lambda x: format(float(x), '.2f') if x is not None else x)
                    df = df[::-1]
                    print(df)
                    if connection_string is not None:
                        await sdk.insert_ofr_data(df)
                elif str(mnemonic).startswith('FNYR'):
                    name = FNYR_OFR[mnemonic]
                    print(name)
                    data = await resp.json()
                    print(data)

                    df = pd.DataFrame(data, columns=['date', 'value'])

                    df['name'] = name
                    df['mnemonic'] = mnemonic
                    df['value'] = df['value'].apply(lambda x: format(float(x), '.2f') if x is not None else x)
                    df = df[::-1]
                    print(df)
                    if connection_string is not None:
                        await sdk.insert_ofr_data(df)


                elif str(mnemonic).startswith('NYPD'):
                    name = NYPD_OFR[mnemonic]
                    print(name)
                    data = await resp.json()
                    print(data)

                    df = pd.DataFrame(data, columns=['date', 'value'])

                    df['name'] = name
                    df['mnemonic'] = mnemonic
                    df['value'] = df['value'].apply(lambda x: format(float(x), '.2f') if x is not None else x)
                    df = df[::-1]
                    print(df)
                    if connection_string is not None:
                        await sdk.insert_ofr_data(df)


asyncio.run(main())