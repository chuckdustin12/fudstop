from dataclasses import dataclass, asdict
from datetime import datetime

import pandas as pd

@dataclass
class JoblessClaimsData:
    date: datetime
    number: int
    percent_change: float


    def as_dataframe(self) -> pd.DataFrame:
        """Converts the object to a pandas DataFrame."""
        df = pd.DataFrame([asdict(self)])
        df.columns = df.columns.str.lower()
        return df
    
    def as_dict(self) -> dict:
        """Converts the object to a dictionary with lowercase keys."""
        return {k.lower(): v for k, v in asdict(self).items()}

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO jobless_claims (date, number, percent_change) VALUES (%s, %s, %s);"""
        cursor.execute(insert_query, (self.date, self.number, self.percent_change))
        conn.commit()
