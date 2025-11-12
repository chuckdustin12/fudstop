from fudstop4.apis.federal_register.fed_register_sdk import FedRegisterSDK



register = FedRegisterSDK()



import asyncio



async def main():

    x = await register.query_document('options')

    print(x.data_dict)
asyncio.run(main())