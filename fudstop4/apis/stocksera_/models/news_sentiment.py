from dataclasses import dataclass, asdict
from datetime import datetime

import pandas as pd


@dataclass
class NewsSentimentData:
    ticker: str
    date: datetime
    title: str
    link: str
    sentiment: str

    def as_dataframe(self) -> pd.DataFrame:
        """Converts the object to a pandas DataFrame."""
        return pd.DataFrame([asdict(self)])

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO news_sentiment (ticker, date, title, link, sentiment) VALUES (%s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.ticker, self.date, self.title, self.link, self.sentiment))
        conn.commit()
