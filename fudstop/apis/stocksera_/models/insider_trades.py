from dataclasses import dataclass, asdict
from datetime import datetime

import pandas as pd

@dataclass
class InsiderTrades:
    ticker: str
    name: str
    relationship: str
    date: datetime
    transaction: str
    cost: float
    shares: int
    value_dollar: float
    shares_total: int
    date_filled: datetime
    empty_string: str  # This field may need special handling


    def as_dataframe(self) -> pd.DataFrame:
        """Converts the object to a pandas DataFrame."""
        return pd.DataFrame([asdict(self)])

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO insider_transaction (ticker, name, relationship, date, transaction, cost, shares, value_dollar, shares_total, date_filled, empty_string) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.ticker, self.name, self.relationship, self.date, self.transaction, self.cost, self.shares, self.value_dollar, self.shares_total, self.date_filled, self.empty_string))
        conn.commit()
