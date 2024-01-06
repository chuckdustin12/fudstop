from dataclasses import dataclass, asdict
from datetime import datetime

import pandas as pd

@dataclass
class ReverseRepoData:
    date: datetime
    amount: float
    num_parties: int
    average: float
    moving_avg: float


    def as_dataframe(self) -> pd.DataFrame:
        """Converts the object to a pandas DataFrame."""
        return pd.DataFrame([asdict(self)])

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO reverse_repo (date, amount, num_parties, average, moving_avg) VALUES (%s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.date, self.amount, self.num_parties, self.average, self.moving_avg))
        conn.commit()
