from typing import List
from polygon.websocket import WebSocketMessage, EquityAgg,EquityQuote,EquityTrade, Market
from apis.polygonio.mapping import stock_condition_dict, STOCK_EXCHANGES, TAPES, quote_conditions, indicators
from datetime import datetime
import asyncio

from list_sets.ticker_lists import most_active_tickers




batch_data_aggs = []
batch_data_trades = []
batch_data_quotes = []
async def handle_stock_msg(msgs: List[WebSocketMessage], data_queue: asyncio.Queue, db=None):

    global batch_data_aggs, batch_data_trades, batch_data_quotes


    for m in msgs:
        if m.symbol in most_active_tickers:




            if isinstance(m, EquityAgg):
                data = {
                    'type': 'EquityAgg',
                    'ticker': m.symbol,
                    'close_price': m.close,
                    'high_price': m.high,
                    'low_price': m.low,
                    'open_price': m.open,
                    'volume': m.volume,
                    'official_open': m.official_open_price,
                    'accumulated_volume': m.accumulated_volume,
                    'vwap_price': m.vwap,
                    'agg_timestamp': datetime.fromtimestamp(m.end_timestamp / 1000.0) if m.end_timestamp is not None else None
                }


                data = {
                    'ticker': m.symbol,
                    'close_price': m.close,
                    'high_price': m.high,
                    'low_price': m.low,
                    'open_price': m.open,
                    'volume': m.volume,
                    'official_open': m.official_open_price,
                    'accumulated_volume': m.accumulated_volume,
                    'vwap_price': m.vwap,
                    'agg_timestamp': datetime.fromtimestamp(m.end_timestamp / 1000.0) if m.end_timestamp is not None else None
                }

                asyncio.create_task(data_queue.put(data))
                # if db is not None:
                #     await db.save_structured_message(data, 'stock_aggs')
                


            elif isinstance(m, EquityTrade):

                data = { 
                    'type': 'EquityTrade',
                    'ticker': m.symbol,
                    'trade_exchange': STOCK_EXCHANGES.get(m.exchange),
                    'trade_price': m.price,
                    'trade_size': m.size,
                    'trade_conditions': [stock_condition_dict.get(condition) for condition in m.conditions] if m.conditions is not None else [],
                    'trade_timestamp': datetime.fromtimestamp(m.timestamp / 1000.0) if m.timestamp is not None else None
                }


                asyncio.create_task(data_queue.put(data))
                # if db is not None:
                #     await db.save_structured_message(data, 'equity_trades')


            elif isinstance(m, EquityQuote):
                timetamp = datetime.fromtimestamp(m.timestamp / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
                timestamp_obj = datetime.strptime(timetamp, '%Y-%m-%d %H:%M:%S')
                data = {
                'type': 'EquityQuote',
                'ticker': m.symbol,
                'ask': m.ask_price,
                'bid':m.bid_price,
                'ask_size': m.ask_size,
                'bid_size':m.bid_size,
                'indicator': [indicators.get(indicator) for indicator in m.indicators] if m.indicators is not None else [],
                'condition':quote_conditions.get(m.condition),

                
                'ask_exchange':STOCK_EXCHANGES.get(m.ask_exchange_id),
                'bid_exchange':STOCK_EXCHANGES.get(m.bid_exchange_id),
                
                'timestamp': timestamp_obj,
                'tape': TAPES.get(m.tape)}
                asyncio.create_task(data_queue.put(data))

                # if db is not None:
                #     await db.insert_equity_quote(data)







