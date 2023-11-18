import os
from dotenv import load_dotenv
load_dotenv()

from fudstop.apis.oic.async_oic_sdk import AsyncOICSDK
db_config = {
    "host": os.environ.get('DB_HOST'), # Default to this IP if 'DB_HOST' not found in environment variables
    "port": int(os.environ.get('DB_PORT')), # Default to 5432 if 'DB_PORT' not found
    "user": os.environ.get('DB_USER'), # Default to 'postgres' if 'DB_USER' not found
    "password": os.environ.get('DB_PASSWORD'), # Use the password from environment variable or default
    "database": os.environ.get('DB_NAME') # Database name for the new jawless database
}
sdk = AsyncOICSDK(**db_config)
import asyncio



async def run_monitor_all():
    await sdk.monitor_all()


asyncio.run(run_monitor_all())
