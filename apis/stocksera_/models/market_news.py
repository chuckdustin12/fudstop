from dataclasses import dataclass, asdict
from datetime import datetime

import pandas as pd

@dataclass
class MarketNewsData:
    date: datetime
    title: str
    source: str
    url: str
    section: str

    def as_dataframe(self) -> pd.DataFrame:
        """Converts the object to a pandas DataFrame."""
        return pd.DataFrame([asdict(self)])

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO market_news (date, title, source, url, section) VALUES (%s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.date, self.title, self.source, self.url, self.section))
        conn.commit()
