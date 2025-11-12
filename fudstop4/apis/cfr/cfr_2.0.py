import aiohttp
import asyncio
import asyncpg
import json
from typing import Optional

class CFRSearchSummaryAPI:
    def __init__(self, base_url: str, db_connection_string: Optional[str] = None):
        self.base_url = base_url
        self.db_connection_string = db_connection_string
        self.session = aiohttp.ClientSession()
        self.db_pool = None

    async def connect_to_db(self):
        if self.db_connection_string:
            self.db_pool = await asyncpg.create_pool(dsn=self.db_connection_string)

    async def create_table(self):
        async with self.db_pool.acquire() as connection:
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS search_summary (
                    id SERIAL PRIMARY KEY,
                    query TEXT,
                    date DATE,
                    last_modified_after DATE,
                    last_modified_on_or_after DATE,
                    last_modified_before DATE,
                    last_modified_on_or_before DATE,
                    response JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (query, date, last_modified_after, last_modified_on_or_after, last_modified_before, last_modified_on_or_before)
                )
            ''')

    async def insert_data(self, data):
        # Flatten the response by converting it to a JSON string
        response_json = json.dumps(data[-1])
        async with self.db_pool.acquire() as connection:
            await connection.execute('''
                INSERT INTO search_summary (
                    query, date, last_modified_after, last_modified_on_or_after, last_modified_before, last_modified_on_or_before, response
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (query, date, last_modified_after, last_modified_on_or_after, last_modified_before, last_modified_on_or_before)
                DO NOTHING
            ''', *(data[:-1] + (response_json,)))

    async def fetch_results(self, query):
        async with self.session.get(f"https://www.ecfr.gov/api/search/v1/results?query=options&per_page=20&page=1&order=relevance") as response:
            print(f"https://www.ecfr.gov/api/api/search/v1/results?query={query}")
            data = await response.json()
            return data     



    async def fetch_summary(self, query):
        #params = {k: v for k, v in params.items() if v is not None}
        async with self.session.get(f"https://www.ecfr.gov/api/search/v1/summary?query={query}") as response:
            data = await response.json()
            return data

    async def fetch_and_store_summary(self, **params):
        data = await self.fetch_summary(**params)
        if self.db_pool:
            await self.insert_data((
                params.get('query'),
                params.get('date'),
                params.get('last_modified_after'),
                params.get('last_modified_on_or_after'),
                params.get('last_modified_before'),
                params.get('last_modified_on_or_before'),
                data
            ))
        return data

    async def close(self):
        await self.session.close()
        if self.db_pool:
            await self.db_pool.close()

async def main():
    # Example usage
    base_url = 'https://example.com'
    db_connection_string = 'postgresql://postgres:fud@localhost:5432/cfr'

    api = CFRSearchSummaryAPI(base_url, db_connection_string)
    # await api.connect_to_db()
    # await api.create_table()

    # Fetch and store summary
    summary_data = await api.fetch_results(query='climate change')
    print(summary_data)

    await api.close()

if __name__ == '__main__':
    asyncio.run(main())