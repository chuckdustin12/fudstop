from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions()

import pandas as pd




import asyncio



async def main():

    await db.connect()


    query  = f"""SELECT heading, type, question, text from vlex_final WHERE text IS NOT NULL AND LENGTH(text) > 0"""

    results = await db.fetch(query)

    df = pd.DataFrame(results, columns=['heading', 'type', 'question', 'text'])

    df.to_csv('all_vlex.csv')

asyncio.run(main())