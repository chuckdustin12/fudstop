from dataclasses import dataclass, asdict
from typing import Dict

import pandas as pd

@dataclass
class InflationData:
    years_data: Dict[int, float]  # Assuming inflation data is float; adjust as needed


    def as_dataframe(self) -> pd.DataFrame:
        """Converts the object to a pandas DataFrame."""
        return pd.DataFrame([asdict(self)])


    def get_inflation_by_year(self, year: int) -> float:
        """Retrieve the inflation value for a specific year."""
        return self.years_data.get(year, None)
    

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        for year, value in self.years_data.items():
            insert_query = """INSERT INTO inflation (year, value) VALUES (%s, %s);"""
            cursor.execute(insert_query, (year, value))
        conn.commit()
