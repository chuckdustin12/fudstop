
from dotenv import load_dotenv
load_dotenv()
import sys
from pathlib import Path
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
from webull_options.webull_options import WebullOptions

db = WebullOptions(database='markets', user='chuck')



import asyncio


async def main():


    await db.db_manager.get_connection()
    all_options = await db.all_options('SPY')
    await db.db_manager.create_table(all_options[0].as_dataframe,table_name='wb_opts', unique_column='option_symbol')
    await db.update_all_options()


asyncio.run(main())