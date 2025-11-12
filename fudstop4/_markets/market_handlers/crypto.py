from polygon.websocket import WebSocketMessage,WebSocketClient, EquityAgg,EquityQuote,EquityTrade, Market
import pytz

from discord_webhook import AsyncDiscordWebhook, DiscordEmbed
import asyncio


from apis.polygonio.mapping import OPTIONS_EXCHANGES, option_condition_desc_dict, option_condition_dict
from datetime import datetime
from math import isnan
from asyncio import Queue
from datetime import timezone
from polygon.websocket import CryptoTrade
from apis.helpers import calculate_price_to_strike, calculate_days_to_expiry, format_large_number, format_large_numbers_in_dataframe,format_large_numbers_in_dict, get_human_readable_string
import requests
import asyncpg
batch_data_aggs = []
batch_data_trades = []


from pytz import timezone
from apis.helpers import convert_to_ns_datetime
utc = timezone('UTC')
aware_datetime = utc.localize(datetime.utcnow())
from .list_sets import crypto_conditions_dict, crypto_exchanges

async def handle_crypto_msg(m: CryptoTrade):
    conditions = [crypto_conditions_dict.get(i) for i in m.conditions]
    data = { 
        'type': m.event_type,
        'symbol': m.pair,
        'exchange': crypto_exchanges.get(m.exchange),
        'id': m.id,
        'price': m.price,
        'size': m.size,
        'conditions': conditions[0],
        'timestamp': convert_to_ns_datetime(m.timestamp)
    }

    yield data
    # await db_manager.save_structured_message(data, 'crypto_trades')
    # # Send to Flask API

    

    #await data_queue.put(data)

