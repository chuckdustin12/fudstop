from dataclasses import dataclass,asdict

import pandas as pd

@dataclass
class LowFloatData:
    rank: int
    ticker: str
    company_name: str
    exchange: str
    previous_close: float
    one_day_change: float
    floating_shares: int
    outstanding_shares: int
    short_int: float
    market_cap: int
    industry: str

    def as_dataframe(self) -> pd.DataFrame:
        """Converts the object to a pandas DataFrame."""
        return pd.DataFrame([asdict(self)])

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO low_float (rank, ticker, company_name, exchange, previous_close, one_day_change, floating_shares, outstanding_shares, short_int, market_cap, industry) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.rank, self.ticker, self.company_name, self.exchange, self.previous_close, self.one_day_change, self.floating_shares, self.outstanding_shares, self.short_int, self.market_cap, self.industry))
        conn.commit()
