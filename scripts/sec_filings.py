import requests
import pandas as pd
import asyncio
from fudstop4.apis.polygonio.polygon_options import PolygonOptions

# Initialize DB Connection
db = PolygonOptions()

# API URL for Federal Register
API_URL = "https://www.federalregister.gov/api/v1/documents.json"

# Query Parameters for SEC Documents
params = {
    "conditions[agencies][]": "securities-and-exchange-commission",
    "order": "newest",  # Get the newest documents first
    "per_page": 100,  # Max per page
    "fields[]": ["document_number", "title", "publication_date", "html_url"]
}

async def fetch_and_store_sec_docs():
    """Fetch SEC-related documents and store them in the database."""
    response = requests.get(API_URL, params=params)

    if response.status_code == 200:
        data = response.json()
        results = data.get("results", [])

        # Convert results into DataFrame
        df = pd.DataFrame([
            {
                "document_number": doc["document_number"],
                "title": doc["title"],
                "publication_date": doc["publication_date"],
                "url": doc["html_url"]
            }
            for doc in results
        ])

        if not df.empty:
            # Connect to DB and upsert
            await db.connect()
            await db.batch_upsert_dataframe(
                df, 
                table_name="sec_filings", 
                unique_columns=["document_number"]  # Ensure uniqueness on document_number
            )
            print(f"✅ Successfully stored {len(df)} SEC documents in the database.")
        else:
            print("⚠️ No SEC documents found.")

    else:
        print(f"❌ API Request Failed: {response.status_code}")

# Run the async function
asyncio.run(fetch_and_store_sec_docs())
