import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)

from polygon.websocket import WebSocketMessage
from apis.helpers import convert_to_ns_datetime
import asyncio
from typing import List



"""
>>> Handles messages from the polygon.io websocket cluster 

"""

async def indices_handler(msg: WebSocketMessage, data_queue: asyncio.Queue, db_manager=None):
    for m in msg:
    # Fetch the name using the incoming symbol

            # database_data = { 
            #     'name': name,
            #     'ticker': m.symbol,
            #     'day_open': m.official_open_price,
            #     'minute_open': m.open,
            #     'minute_high': m.high,
            #     'minute_low': m.low,
            #     'minute_close': m.close,
            #     'minute_start': convert_to_ns_datetime(m.start_timestamp),
            #     'minute_end': convert_to_ns_datetime(m.end_timestamp)
            # }

            data_queue_data = { 
                'type': 'Indices',
                'ticker': m.symbol,
                'day_open': m.official_open_price,
                'minute_open': m.open,
                'minute_high': m.high,
                'minute_low': m.low,
                'minute_close': m.close,
                'minute_start': convert_to_ns_datetime(m.start_timestamp),
                'minute_end': convert_to_ns_datetime(m.end_timestamp)
            }

            # if db_manager is not None:
            #     asyncio.create_task(db_manager.save_structured_message(data, "indices_aggs"))

            #await db.insert_indices_aggs(data)
            await data_queue.put(data_queue_data)
        