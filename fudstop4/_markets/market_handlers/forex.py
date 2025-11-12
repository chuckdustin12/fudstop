from polygon.websocket import WebSocketMessage
import asyncio
from datetime import datetime
from datetime import timezone

import asyncpg
batch_data_aggs = []
batch_data_trades = []

from pytz import timezone
from apis.helpers import convert_to_ns_datetime
import pandas as pd
utc = timezone('UTC')
aware_datetime = utc.localize(datetime.utcnow())


"""

>>> Handles Forex Messages from the polygon.io websocket.

"""

async def handle_forex_msg(m, db):
    data_quotes= { 
        'ask': m.ask_price,
        'bid': m.bid_price,
        'pair': m.pair,
        'timestamp': convert_to_ns_datetime(m.timestamp)

    }
    df = pd.DataFrame()
    await db.batch_insert_dataframe(df, table_name='forex_aggs', unique_columns='insertion_timestamp')

    yield data_quotes
