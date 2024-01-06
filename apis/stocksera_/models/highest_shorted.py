from dataclasses import dataclass, asdict
from datetime import datetime

import pandas as pd

@dataclass
class HighestShortedData:
    rank: int
    ticker: str
    date: datetime
    short_interest: int
    average_volume: int
    days_to_cover: float
    percent_float_short: float


    def as_dataframe(self) -> pd.DataFrame:
        """Converts the object to a pandas DataFrame."""
        return pd.DataFrame([asdict(self)])
    

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO highest_shorted (rank, ticker, date, short_interest, average_volume, days_to_cover, percent_float_short) VALUES (%s, %s, %s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.rank, self.ticker, self.date, self.short_interest, self.average_volume, self.days_to_cover, self.percent_float_short))
        conn.commit()
