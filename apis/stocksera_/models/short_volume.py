from dataclasses import dataclass, asdict
from datetime import datetime

import pandas as pd

@dataclass
class ShortVolumeData:
    date: datetime
    ticker: str
    short_vol: int
    short_exempt_vol: int
    total_vol: int
    percent_shorted: float

    def as_dataframe(self) -> pd.DataFrame:
        """Converts the object to a pandas DataFrame."""
        return pd.DataFrame([asdict(self)])

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO short_volume (date, ticker, short_vol, short_exempt_vol, total_vol, percent_shorted) VALUES (%s, %s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.date, self.ticker, self.short_vol, self.short_exempt_vol, self.total_vol, self.percent_shorted))
        conn.commit()
