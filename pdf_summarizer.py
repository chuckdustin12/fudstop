from fudstop4.apis.polygonio.polygon_options import PolygonOptions


opts = PolygonOptions()


import asyncio

import pandas as pd


async def main():
    await opts.connect()
    query = f"""SELECT pdf_url from new_documents"""

    results = await opts.fetch(query)


    for url in results:

        