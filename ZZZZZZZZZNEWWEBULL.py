import asyncio
import aiohttp
import os
from dotenv import load_dotenv
load_dotenv()
from helpers import generate_webull_headers
import pandas as pd
from fudstop4.apis.polygonio.polygon_options import PolygonOptions

db = PolygonOptions()

class Groups:

    def __init__(self, groups):
        self.type_dict = {'Equity Index Futures':'equityIndex',
                          'FX Futures':'fx',
                          'Metals Futures':'metal',
                          'Energy Futures':'energy',
                          'Interest Rates Futures':'interestRate',
                          'Agricultural Futures':'agricultural',
                          'Crypto Futures':'coinbaseCryptocurrency',
                          'Micro Futures':'micro'}


        self.id = [i.get('id') for i in groups]
        self.name = [i.get('name') for i in groups]
        self.type = [i.get('type') for i in groups]
        self.tabs = [i.get('tabs') for i in groups]
        self.data_dict = { 
            'id': self.id,
            'name': self.name,
            'type': self.type,

        }

        self.as_dataframe = pd.DataFrame(self.data_dict)


class Tabs:
    def __init__(self, tabs):


        self.tabs_dict = {'E-mini Dow': 'equityIndex.YM',
'E-mini Nasdaq': 'equityIndex.NQ',
'E-mini S&P': 'equityIndex.ES',
'E-mini Russell 2000': 'equityIndex.RTY',
'E-mini S&P MidCap 400': 'equityIndex.EMD',
'Micro E-mini S&P 500': 'equityIndex.MES',
'Micro E-mini Nasdaq-100': 'equityIndex.MNQ',
'Micro E-mini Russell 2000': 'equityIndex.M2K',
'Micro E-mini Dow': 'equityIndex.MYM',
'EUR/USD': 'fx.6E',
'JPY/USD': 'fx.6J',
'GBP/USD': 'fx.6B',
'AUD/USD': 'fx.6A',
'Micro AUD/USD': 'fx.M6A',
'Micro GBP/USD': 'fx.M6B',
'CAD/USD': 'fx.6C',
'Micro CAD/USD': 'fx.MCD',
'E-mini EUR/USD': 'fx.E7',
'Micro EUR/USD': 'fx.M6E',
'E-mini JPY/USD': 'fx.J7',
'CHF/USD': 'fx.6S',
'Micro CHF/USD': 'fx.MSF',
'Mexican peso': 'fx.6M',
'New Zealand': 'fx.6N',
'Gold': 'metal.GC',
'Silver': 'metal.SI',
'Platinum': 'metal.PL',
'E-mini Gold': 'metal.QO',
'Micro Gold': 'metal.MGC',
'Copper': 'metal.HG',
'Micro Copper': 'metal.MHG',
'E-mini Copper': 'metal.QC',
'Micro Silver': 'metal.SIL',
'E-mini Silver': 'metal.QI',
'Palladium': 'metal.PA',
'1-Ounce Gold': 'metal.1OZ',
'Crude Oil': 'energy.CL',
'Natural Gas (Henry Hub)': 'energy.NG',
'RBOB Gasoline': 'energy.RB',
'Brent Crude Oil': 'energy.BZ',
'E-mini Crude Oil': 'energy.QM',
'Micro WTI Crude Oil': 'energy.MCL',
'NY Harbor ULSD': 'energy.HO',
'E-mini Natural Gas': 'energy.QG',
'Micro Henry Hub Natural Gas': 'energy.MNG',
'2-Year T-Note': 'interestRate.ZT',
'5-Year T-Note': 'interestRate.ZF',
'10-Year T-Note': 'interestRate.ZN',
'Ultra 10-Year T-Note': 'interestRate.TN',
'Micro 10-Year Yield': 'interestRate.10Y',
'Ultra T-Bond': 'interestRate.UB',
'U.S. Treasury Bond': 'interestRate.ZB',
'Micro 30-Year Yield': 'interestRate.30Y',
'Micro 2-Year Yield': 'interestRate.2YY',
'Micro 5-Year Yield': 'interestRate.5YY',
'Soybeans': 'agricultural.ZS',
'Chicago Wheat': 'agricultural.ZW',
'Corn': 'agricultural.ZC',
'Feeder Cattle': 'agricultural.GF',
'Lean Hogs': 'agricultural.HE',
'Live Cattle': 'agricultural.LE',
'Mini-Corn': 'agricultural.XC',
'Mini Soybean': 'agricultural.XK',
'Soybean Oil': 'agricultural.ZL',
'Soybean Meal': 'agricultural.ZM',
'Oats': 'agricultural.ZO',
'Mini-sized Chicago Wheat': 'agricultural.XW',
'Bitcoin - CME': 'cryptocurrency.CME_BTC',
'Micro Bitcoin - CME': 'cryptocurrency.CME_MBT',
'Ether - CME': 'cryptocurrency.CME_ETH',
'Micro Ether - CME': 'cryptocurrency.CME_MET',
'Bitcoin - CDE': 'cryptocurrency.CDE_BTI',
'Nano Bitcoin - CDE': 'cryptocurrency.CDE_BIT',
'Ether - CDE': 'cryptocurrency.CDE_ETI',
'Nano Ether- CDE': 'cryptocurrency.CDE_ET',
'Micro E-mini Dow': 'micro.MYM',
'Micro E-mini S&P 500': 'micro.MES',
'Micro E-mini Nasdaq-100': 'micro.MNQ',
'Micro E-mini Russell 2000': 'micro.M2K',
'Micro AUD/USD': 'micro.M6A',
'Micro GBP/USD': 'micro.M6B',
'Micro CAD/USD': 'micro.MCD',
'Micro EUR/USD': 'micro.M6E',
'Micro CHF/USD': 'micro.MSF',
'Micro Gold': 'micro.MGC',
'Micro Copper': 'micro.MHG',
'Micro Silver': 'micro.SIL',
'Micro WTI Crude Oil': 'micro.MCL',
'Micro 10-Year Yield': 'micro.10Y',
'Micro 30-Year Yield': 'micro.30Y',
'Micro 2-Year Yield': 'micro.2YY',
'Micro 5-Year Yield': 'micro.5YY',
'Micro Bitcoin': 'micro.MBT',
'Micro Ether': 'micro.MET',
'Micro Henry Hub Natural Gas': 'micro.MNG',}

        self.id = [i.get('id') for i in tabs]
        self.name = [i.get('name') for i in tabs]
        self.type = [i.get('type') for i in tabs]
        self.data_dict = { 
            'id': self.id,
            'name': self.name,
            'type': self.type,

        }

        self.as_dataframe = pd.DataFrame(self.data_dict)



class FuturesData:
    def __init__(self, data):

        self.tickerId = [i.get('tickerId') for i in data]
        self.contractSpecsId = [i.get('contractSpecsId') for i in data]
        self.relatedContractId = [i.get('relatedContractId') for i in data]
        self.symbol = [i.get('symbol') for i in data]
        self.exchangeId = [i.get('exchangeId') for i in data]
        self.name = [i.get('name') for i in data]
        self.securityType = [i.get('securityType') for i in data]
        self.securitySubType = [i.get('securitySubType') for i in data]
        self.type = [i.get('type') for i in data]
        self.currencyId = [i.get('currencyId') for i in data]
        self.regionId = [i.get('regionId') for i in data]
        self.month = [i.get('month') for i in data]
        self.year = [i.get('year') for i in data]
        self.lastTradingDate = [i.get('lastTradingDate') for i in data]
        self.firstTradingDate = [i.get('firstTradingDate') for i in data]
        self.settlementDate = [i.get('settlementDate') for i in data]
        self.expDate = [i.get('expDate') for i in data]
        self.contractSize = [i.get('contractSize') for i in data]
        self.contractUnit = [i.get('contractUnit') for i in data]
        self.futuresSpecs = [i.get('futuresSpecs') for i in data]
        self.disSymbol = [i.get('disSymbol') for i in data]
        self.disExchangeCode = [i.get('disExchangeCode') for i in data]
        self.exchangeCode = [i.get('exchangeCode') for i in data]
        self.template = [i.get('template') for i in data]
        self.minPriceFluctuation = [i.get('minPriceFluctuation') for i in data]
        self.contractType = [i.get('contractType') for i in data]
        self.futType = [i.get('futType') for i in data]
        self.marginInitialCash = [i.get('marginInitialCash') for i in data]
        self.contractStatus = [i.get('contractStatus') for i in data]
        self.mult = [i.get('mult') for i in data]

        self.data_dict = { 
            'ticker_id': self.tickerId,
            'contract_specs_id': self.contractSpecsId,
            'related_contract_id': self.relatedContractId,
            'symbol': self.symbol,
            'name': self.name,
            'month': self.month,
            'year': self.year,
            'last_trading_date': self.lastTradingDate,
            'first_trading_date': self.firstTradingDate,
            'settlement_date': self.settlementDate,
            'expiry': self.expDate,
            'contract_size': self.contractSize,
            'contract_unit': self.contractUnit,
            'min_price_fluctuation': self.minPriceFluctuation,
            'contract_type': self.contractType,
            'fut_type': self.futType,
            'margin_initial_cash': self.marginInitialCash,
            'mult': self.mult

        }

        self.as_dataframe = pd.DataFrame(self.data_dict)


async def main():
    await db.connect()

    url = f"https://quotes-gw.webullfintech.com/api/bgw/market/futures-main-contracts?brokerId=8&regionId=6"


    async with aiohttp.ClientSession(headers=generate_webull_headers()) as session:

        async with session.get(url) as resp:

            data = await resp.json()

            groups = data['groups']
            groups = Groups(groups)

            names = groups.name
            ids = groups.id
            types = groups.type

            tabs = [i.get('tabs') for i in data['groups']]
            tabs = [item for sublist in tabs for item in sublist]
            data = [item for tab in tabs for item in tab.get('data', [])]

            tabs = Tabs(tabs)

            data = FuturesData(data)

            await db.batch_upsert_dataframe(data.as_dataframe, 'futures_contracts', unique_columns=['symbol'])

asyncio.run(main())