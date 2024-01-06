from polygon.websocket import WebSocketMessage
import asyncio
from datetime import datetime
from datetime import timezone

import asyncpg
batch_data_aggs = []
batch_data_trades = []

from pytz import timezone
from apis.helpers import convert_to_ns_datetime
utc = timezone('UTC')
aware_datetime = utc.localize(datetime.utcnow())


"""

>>> Handles Forex Messages from the polygon.io websocket.

"""

async def handle_forex_msg(msgs: WebSocketMessage, data_queue: asyncio.Queue, db_manager= None):
    for m in msgs:
        print(m)
        if m.event_type == 'C':
            data_quotes= { 
                'ask': m.ask_price,
                'bid': m.bid_price,
                'pair': m.pair,
                'timestamp': convert_to_ns_datetime(m.timestamp)

            }
            if db_manager is not None:
                await db_manager.save_structured_message(data_quotes,'forex_quotes')
            await data_queue.put(data_quotes)


        elif m.event_type == 'CA':
            print(m)