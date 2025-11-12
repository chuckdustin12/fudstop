from dataclasses import dataclass
from datetime import datetime

@dataclass
class BorrowedSharesData:
    ticker: str
    fee: float
    available: int
    date_updated: datetime

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO borrowed_shares (ticker, fee, available, date_updated) VALUES (%s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.ticker, self.fee, self.available, self.date_updated))
        conn.commit()
