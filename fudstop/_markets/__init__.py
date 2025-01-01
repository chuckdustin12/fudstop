

from collections import deque
from dotenv import load_dotenv

load_dotenv()

from typing import List



class Inserts:
    def __init__(self, host:str='localhost', user:str='chuck', password:str='fud', port:int=5432, database:str='markets'):
        self.host=host
        self.user=user
        self.password=password
        self.port=port
        self.database=database





    async def insert_theta_resistant(self, data):
        query = '''
            INSERT INTO options_data (
                underlying_symbol, strike, call_put, expiry, open, high, low, close, vwap,
                change_percent, oi, vol, vol_oi, iv, iv_percentile, intrinsic_value,
                extrinsic_value, time_value, bid, mid, ask, spread, spread_pct,
                trade_size, trade_price, trade_exchange, moneyness, velocity, profit_potential,
                delta, delta_theta_ratio, gamma, gamma_risk, vega, vega_impact, theta, theta_decay
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                      $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37)
        '''
        await self.conn.execute(query, *data)

    async def close_connection(self):
        await self.conn.close()
