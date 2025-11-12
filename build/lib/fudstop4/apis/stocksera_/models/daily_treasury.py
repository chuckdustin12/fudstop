from dataclasses import dataclass, asdict
from datetime import datetime

import pandas as pd

@dataclass
class DailyTreasuryData:
    date: datetime
    close_balance: float
    open_balance: float
    amount_change: float
    percent_change: float
    moving_avg: float

    def as_dataframe(self) -> pd.DataFrame:
        """Converts the object to a pandas DataFrame."""
        return pd.DataFrame([asdict(self)])
    

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO daily_treasury (date, close_balance, open_balance, amount_change, percent_change, moving_avg) VALUES (%s, %s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.date, self.close_balance, self.open_balance, self.amount_change, self.percent_change, self.moving_avg))
        conn.commit()
