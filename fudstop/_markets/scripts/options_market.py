
import sys
from pathlib import Path
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import os
from dotenv import load_dotenv
load_dotenv()
from discord_webhook import AsyncDiscordWebhook, DiscordEmbed
from _markets.webhook_dicts import option_conditions_hooks
from _markets.embeddings import sized_trade_embed, specials_embed_2, dip_specials_embed, index_surge_embed, dip_specials_embed_2_super, theta_resistant_embed
from polygon.websocket import WebSocketClient, WebSocketMessage, Market, Feed
from apis.polygonio.polygon_options import PolygonOptions
from fudstop.apis.webull.webull_options.webull_options import WebullOptions



from _markets.list_sets.dicts import hex_color_dict as hex_colors

from apis.polygonio.polygon_database import PolygonDatabase
import asyncio
from datetime import datetime
from finished.db_manager import DbManager
from market_handlers.options import handle_option_msg
from embeddings import option_condition_embed
from apis.polygonio.async_polygon_sdk import Polygon
from apis.webull.webull_trading import WebullTrading

import pytz
from collections import deque
import aiohttp


class OptionsMarket:
    def __init__(self, database:str='markets', user:str='chuck', password:str='fud', port:int=5432, host:str='localhost'):

        self.webull = WebullTrading()
        self.polygon = Polygon(database=database,user=user,password=password,port=port,host=host)
        self.poly_opts = PolygonOptions(user=user, database=database, host=host, port=port, password=password)
        self.wb_opts = WebullOptions(user='chuck', database='market_data')
        self.conn = None
        self.pool = None
        self.test = os.environ.get('test')
        self.osob_1m = os.environ.get('osob_minute')
        self.osob_1h = os.environ.get('osob_hour')
        self.osob_1d = os.environ.get('osob_day')
        self.osob_1w = os.environ.get('osob_week')
        self.osob_1mth = os.environ.get('osob_mth')
        self.oi_5k10k = os.environ.get('oi_5k10k')
        self.specials = os.environ.get('specials')
        self.specials_2 = os.environ.get('specials_2')
        self.dip_specials = os.environ.get('dip_specials')
        self.uoa = os.environ.get('unusual_options')

        self.now_utc = datetime.now(pytz.utc)
        self.eastern = pytz.timezone('US/Eastern')
        self.now_eastern = self.now_utc.astimezone(self.eastern)
        self.formatted_time = self.now_eastern.strftime('%Y-%m-%d %H:%M:%S')
        self.new_high = os.environ.get('new_high')
        self.new_low = os.environ.get('new_low')
        self.spx_indices = os.environ.get('spx_indices')
        self.nasdaq_indices = os.environ.get('nasdaq_indices')
        self.dow_indices = os.environ.get('dow_indices')
        self.crypto_10k_buys = os.environ.get('crypto_10k_buys')
        self.crypto_10k_sells = os.environ.get('crypto_10k_sells')
        self.fire_sale = os.environ.get('fire_sale')
        self.accumulation = os.environ.get('accumulation')
        self.neutral_zone = os.environ.get('neutral_zone')
        self.near_52_high = os.environ.get('near_52_high')
        self.near_52_low = os.environ.get('near_52_low')
        self.cost_dist_98 = os.environ.get('cost_dist_98')
        self.cost_dist_02 = os.environ.get('cost_dist_02')
        self.index_surge = os.environ.get('index_surge')
        self.equity_option_trade_deque = deque()



    async def process_webull_volume_analysis(self, deriv_id):
        async with aiohttp.ClientSession(headers=self.wb_opts.headers) as session:
            url=f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={deriv_id}"
            async with session.get(url) as resp:
                datas = await resp.json()

            
                totalNum = datas['totalNum'] if 'totalNum' in datas else None
                totalVolume = datas['totalVolume'] if 'totalVolume' in datas else None
                avgPrice = datas['avgPrice']if 'avgPrice' in datas else None
                buyVolume = datas['buyVolume']if 'buyVolume' in datas else None
                sellVolume = datas['sellVolume']if 'sellVolume' in datas else None
                neutralVolume = datas['neutralVolume']if 'neutralVolume' in datas else None

                # Calculating percentages and ratios
                buyPct = (buyVolume / totalVolume) * 100 if totalVolume else 0
                sellPct = (sellVolume / totalVolume) * 100 if totalVolume else 0
                nPct = (neutralVolume / totalVolume) * 100 if totalVolume else 0

                buyRatio = buyVolume / totalVolume if totalVolume else 0
                sellRatio = sellVolume / totalVolume if totalVolume else 0
                neutralRatio = neutralVolume / totalVolume if totalVolume else 0


                dates = datas['dates'] if 'dates' in datas else None
                trade_data = datas['datas'] if 'datas' in datas else None


                volume_analysis_data =  {
                    'num_trades': totalNum,
                    'total_volume': totalVolume,
                    'avg_price': avgPrice,
                    'buy_volume': buyVolume,
                    'sell_volume': sellVolume,
                    'neut_volume': neutralVolume,
                    'buy_ratio': buyRatio,
                    'sell_ratio': sellRatio,
                    'neut_ratio': neutralRatio,
                    'buy_pct': buyPct,
                    'sell_pct': sellPct,
                    'neut_pct': nPct,
                    
                    
                }

                yield trade_data, volume_analysis_data


    async def consumer(self, queue: asyncio.Queue, conn):

        batch = []

        while True:
            data = await queue.get()
            print(queue.qsize())

            


            type = data.get('type')





            if type =='EquityOptionAgg':

                option_symbol = data.get('option_symbol')
                underlying_symbol = data.get('underlying_symbol')
                strike = data.get('strike')
                expiry = data.get('expiry')
                call_put = data.get('call_put')
                total_volume = data.get('total_volume')
                day_vwap = data.get('day_vwap')
                volume = data.get('volume')
                official_open = data.get('official_open')
                last_price = data.get('last_price')
                wb_symbol = option_symbol.replace('O:', '')
                price_vwap_diff=  data.get('price_vwap_diff')
                price_diff = data.get('price_diff')
                price_change = data.get('price_percent_change')
                moneyness = data.get('moneyness')
                volume_to_price = data.get('volume_to_price')
                volume_percent_total = data.get('volume_percent_total')

            


                if volume >= 100:
                    exists, deriv_id = await self.wb_opts.db_manager.get_ticker_id_by_symbol(wb_symbol)
                    if exists == True:
                        async for trade_data, volume_analysis_dict in self.process_webull_volume_analysis(deriv_id):
                            if trade_data is not None:
                                # Initialize variables to store total buy and sell volumes
                                total_neutral_volume = 0
                                total_buy_volume = 0
                                total_sell_volume = 0
                                for data in trade_data:
                                    total_buy_volume += data.get('buy', 0)
                                    total_sell_volume += data.get('sell', 0)
                                    total_neutral_volume += data.get('neutral', 0)  # Assuming 'neutral' is the key for neutral volume

                                # Format the totals for the embed, now including total neutral volume
                                buy_sell_vol_str = f"> Total Buy Volume: **{total_buy_volume}**\n" \
                                                f"> Total Sell Volume: **{total_sell_volume}**\n" \
                                                f"> Total Neutral Volume: **{total_neutral_volume}**"
                                neutralRatio = volume_analysis_dict.get('neut_ratio')
                                buyRatio = volume_analysis_dict.get('buy_ratio')
                                sellRatio = volume_analysis_dict.get('sell_ratio')          
                                buyVolume = volume_analysis_dict.get('buy_volume')            
                                sellVolume = volume_analysis_dict.get('sell_volume')    
                                neutralVolume = volume_analysis_dict.get('neut_volume')                     
                                buyPct = volume_analysis_dict.get('buy_pct') 
                                sellPct = volume_analysis_dict.get('sell_pct')         
                                nPct = volume_analysis_dict.get('neut_pct')  
                                avgPrice = volume_analysis_dict.get('avg_price')
                                totalNum = volume_analysis_dict.get('num_trades')


                                # Create a list of dropdown options
                                if neutralRatio >= 0.65 and total_volume >= 1000 and total_buy_volume >= total_sell_volume:
                                    hook = AsyncDiscordWebhook(self.dip_specials, content=f"@everyone - TEST {underlying_symbol} | ${strike} | {call_put} | {expiry}")
                                    await hook.execute()





                                if total_buy_volume >= 500 and total_buy_volume <= 1400 and total_buy_volume >= total_sell_volume * 10 and volume_percent_total >= 50:
                                    
                                    asyncio.create_task(specials_embed_2(underlying_symbol=underlying_symbol, strike=strike,call_put=call_put, expiry=expiry,buyPct=buyPct,nPct=nPct, sellPct=sellPct, official_open=official_open, last_price=last_price, day_vwap=day_vwap, buyVolume=buyVolume,neutralVolume=neutralVolume, sellVolume=sellVolume,volume_percent_total=volume_percent_total,volume=volume,avgPrice=avgPrice,totalNum=totalNum,moneyness=moneyness,price_diff=price_diff,price_change=price_change,volume_to_price=volume_to_price,buy_sell_vol_str=buy_sell_vol_str))

                                    # asyncio.create_task(self.opts.insert_specials(underlying_symbol,strike,call_put,expiry,buyPct,nPct,sellPct,official_open,last_price,day_vwap,buyVolume,neutralVolume,sellVolume,volume,avgPrice,volume_percent_total,totalNum,moneyness,price_diff,price_change,volume_to_price, deriv_id))





                                if total_buy_volume >= 2000 and total_buy_volume >= total_sell_volume * 10 and price_change is not None and round(float(price_change),2) <= -65:
                                    asyncio.create_task(dip_specials_embed(underlying_symbol,strike,call_put,expiry,buyPct,nPct,sellPct,official_open,last_price,day_vwap,buyVolume,neutralVolume,sellVolume,volume,avgPrice,volume_percent_total,totalNum,moneyness,price_diff,price_change,volume_to_price,buy_sell_vol_str))







                                if underlying_symbol in ['SPY', 'IWM', 'SPX', 'NDX', 'QQQ', 'TQQQ', 'SQQQ'] and volume >= 1500 and buyRatio is not None and buyRatio >= 0.85:
                                    asyncio.create_task(index_surge_embed(underlying_symbol,strike,expiry,call_put,buyPct,nPct,sellPct,buyRatio, neutralRatio, sellRatio, totalNum,moneyness,price_diff,price_change,volume_to_price))

            
        
            if type == 'EquityOptionTrade':
                collected_symbols = ""
                symbol_count = 0
                batch_size = 250

                expiry = data.get('expiry')
                option_symbol = data.get('option_symbol')
                # Add the symbol to the string, separated by a comma
                wb_symbol = data.get('option_symbol').replace('O:', '')
                call_put = data.get('call_put')
                strike = float(data.get('strike'))
                underlying_symbol = data.get('underlying_symbol')
                option_symbol = data.get('option_symbol')
                price = data.get('price')
                price_change = data.get('price_change')
                size = data.get('size')
                volume_change = data.get('volume_change')
                conditions = data.get('conditions')
                exchange = data.get('exchange')
                price_to_strike = data.get('price_to_strike')
                hour_of_day = data.get('hour_of_day')
                weekday = data.get('weekday')
                timestamp = data.get('timestamp')
                dollar_cost = (100 * price) * size
                formatted_date = expiry.strftime('%Y-%m-%d')
                print(formatted_date)

                if size is not None and size >= 500 and conditions in option_conditions_hooks:
                    hook = option_conditions_hooks[conditions]
                    print(data)
                    asyncio.create_task(option_condition_embed(price_to_strike=price_to_strike,conditions=conditions,option_symbol=option_symbol,underlying_symbol=underlying_symbol,strike=strike,call_put=call_put,price=price,size=size,exchange=exchange,volume_change=volume_change,price_change=price_change,weekday=weekday,hour_of_day=hour_of_day, expiry=formatted_date,hook=hook, conn=conn))



                if size is not None and size >= 500 and size <= 999:
                    asyncio.create_task(sized_trade_embed(dollar_cost=dollar_cost,expiry=formatted_date,option_symbol=option_symbol,call_put=call_put,strike=strike,underlying_symbol=underlying_symbol,price=price,price_change=price_change,size=size,volume_change=volume_change,conditions=conditions,exchange=exchange,price_to_strike=price_to_strike,hour_of_day=hour_of_day,weekday=weekday,timestamp=timestamp, conn=conn))


                if size is not None and size >= 1000 and size <= 9999:
                    asyncio.create_task(sized_trade_embed(dollar_cost=dollar_cost,expiry=formatted_date,option_symbol=option_symbol,call_put=call_put,strike=strike,underlying_symbol=underlying_symbol,price=price,price_change=price_change,size=size,volume_change=volume_change,conditions=conditions,exchange=exchange,price_to_strike=price_to_strike,hour_of_day=hour_of_day,weekday=weekday,timestamp=timestamp, conn=conn))


                if size is not None and size >= 10000 and size <= 49999:
                    asyncio.create_task(sized_trade_embed(dollar_cost=dollar_cost,expiry=formatted_date,option_symbol=option_symbol,call_put=call_put,strike=strike,underlying_symbol=underlying_symbol,price=price,price_change=price_change,size=size,volume_change=volume_change,conditions=conditions,exchange=exchange,price_to_strike=price_to_strike,hour_of_day=hour_of_day,weekday=weekday,timestamp=timestamp, conn=conn))

                if size is not None and size >= 50000:
                    asyncio.create_task(sized_trade_embed(dollar_cost=dollar_cost,expiry=formatted_date,option_symbol=option_symbol,call_put=call_put,strike=strike,underlying_symbol=underlying_symbol,price=price,price_change=price_change,size=size,volume_change=volume_change,conditions=conditions,exchange=exchange,price_to_strike=price_to_strike,hour_of_day=hour_of_day,weekday=weekday,timestamp=timestamp, conn=conn))

                
                if size >= 100:
                    exists, deriv_id = await self.wb_opts.db_manager.get_ticker_id_by_symbol(wb_symbol)
                    if exists == True:
                        batch.append(deriv_id)


                    if len(batch) >= 5:

                        batch_str = ','.join(map(str, batch))
                        
                        async with aiohttp.ClientSession(headers=self.wb_opts) as session:
                            url=f"https://quotes-gw.webullfintech.com/api/quote/option/quotes/queryBatch?derivativeIds={batch_str}"
                            async with session.get(url) as resp:
                                data = await resp.json()

                                for i in data:
                                    open = i.get('open', None)
                                    high = i.get('high', None)
                                    low = i.get('low', None)
                                    close = i.get('close', None)
                                    strike = i.get('strikePrice', None)
                                    preclose = i.get('preClose', None)
                                    volume = i.get('volume', None)
                                    latest_vol = i.get('latestPriceVol', None)
                                    delta = i.get('delta', None)
                                    gamma = i.get('gamma', None)
                                    theta = i.get('theta', None)
                                    rho = i.get('rho', None)
                                    vega = i.get('vega', None)
                                    deriv_id = i.get('tickerId', None)
                                    change = i.get('change', None)
                                    change_ratio = i.get('changeRatio', None)
                                    expiry = i.get('expireDate', None)
                                    oi = i.get('openInterest', None)
                                    oi_change = i.get('openIntChange', None)
                                    under_sym = i.get('unSymbol')
                                    call_put = i.get('direction')
                                    data = {
                                        'open': open,
                                        'high': high,
                                        'low': low,
                                        'close': close,
                                        'strike': strike,
                                        'preclose': preclose,
                                        'volume': volume,
                                        'latest_vol': latest_vol,
                                        'delta': delta,
                                        'gamma': gamma,
                                        'theta': theta,
                                        'rho': rho,
                                        'vega': vega,
                                        'deriv_id': deriv_id,
                                        'change': change,
                                        'change_ratio': change_ratio,
                                        'expiry': expiry,
                                        'oi': oi,
                                        'oi_change': oi_change,
                                        'under_sym': under_sym,
                                        'call_put': call_put
                                    }

                                    if volume is not None:
       

                                        if float(theta) >= -0.04:

                                            await theta_resistant_embed(data)

                                            



import logging

logging.basicConfig(level=logging.INFO)


options = OptionsMarket(database='market_data')

async def run_main_tasks(conn):
    num_workers = int(os.environ.get("NUM_WORKERS", 12))
    polygon_key = os.environ.get('YOUR_POLYGON_KEY')
   

    logging.info("Starting up...")

    data_queue = asyncio.Queue()
    consumer_tasks = [asyncio.create_task(options.consumer(data_queue, conn)) for _ in range(num_workers)]

    client = WebSocketClient(api_key=polygon_key, subscriptions=['T.*', 'A.*'], market=Market.Options)
    websocket_task = asyncio.create_task(client.connect(lambda msgs, handler=handle_option_msg: handler(msgs, data_queue)))

    await asyncio.gather(*consumer_tasks, websocket_task, return_exceptions=True)

async def main():
    conn = await options.wb_opts.db_manager.get_connection()

    while True:
        try:
            await run_main_tasks(conn)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            
            logging.info("Restarting main tasks...")
            await asyncio.sleep(1)
            continue

if __name__ == "__main__":
    asyncio.run(main())