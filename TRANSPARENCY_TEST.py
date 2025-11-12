from fudstop4.apis.transparencyusa.transparencyusa_sdk import TransparencyUSASDK


sdk = TransparencyUSASDK()


import asyncio


async def main():


    x = await sdk.get_all_individuals()

    x.to_csv('all_individuals.csv')


asyncio.run(main())