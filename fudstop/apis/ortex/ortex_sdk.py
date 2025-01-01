import pandas as pd
import httpx
import asyncio

from .models.ortex_models import ShortScores, Options, Events, Signals, ShortInterestRows, ShortInterestColumns, NewsColumns,NewsRows, SectorShorts


class OrtexSDK:
    def __init__(self):
        self.cookie = { }



    async def test(self):

        endpoint = f"https://ortex-gui.ortex.com/interface/api/universe/100/moves?GUIv=2&page_size=5&page=1&days_back=0&type=fallers"

        async with httpx.AsyncClient(headers=None) as client:
            data = await client.get(endpoint)
            data = data.json()

            print(data)


    async def sector_averages(self, page_size:str='50', page:str='1', days_back:str='1'):

        endpoint=f"https://ortex-gui.ortex.com/API/v2/7/short/list/sector_averages?&dgformat&GUIv=2&page_size={page_size}&page={page}&initial=true&days_back={days_back}"

        async with httpx.AsyncClient(headers=None) as client:
            data = await client.get(endpoint)
            data = data.json()



            rows = data.get('rows')


            sector_shorts = SectorShorts(rows)

            return sector_shorts

    async def fetch_data(self):

        """RETURNS
        
        >>> short_scores, options, events, and signals.
        
        >>> USAGE <<<

        >>> shorts, options, events, signals = await ortex.fetch_data()
        """

        endpoint = f"https://ortex-gui.ortex.com/interface/api/morning_note/data?GUIv=2"

        async with httpx.AsyncClient(headers=None) as client:
            data = await client.get(endpoint)


            data = data.json()


            date_time = data.get('date_time')
            events = data.get('events')
            short_scores = data.get('short_scores')
            options = data.get('options')
            trading_signal = data.get('trading_signals')



            short_scores = ShortScores(short_scores)

            options = Options(options)

            events = Events(events)

            signals = Signals(trading_signal)



            return short_scores, options, events, signals



    async def lending_changes(self, page_size:str='25', page:str='1', days_back:str='1'):

        async with httpx.AsyncClient(headers=None) as client:
            data = await client.get(f"https://ortex-gui.ortex.com/API/v2/7/short/list/largest_lending_changes?&dgformat&GUIv=2&page_size={page_size}&page={page}&initial=true&days_back={days_back}")

            data = data.json()
            columns = data.get('columns')
            rows = data.get('rows')
            columnVisibilityModel = data.get('columnVisibilityModel')
            length = data.get('length')
            paginationLinks = data.get('paginationLinks')
            metadata = data.get('metadata')
            obfuscated = data.get('obfuscated')
            obfuscation_reason = data.get('obfuscation_reason')


            columns = ShortInterestColumns(columns)
            rows = ShortInterestRows(rows)

            return columns, rows


    async def news(self,page_size:str='50',min_impact:str='7', page_number:str='1', size=500):

        async with httpx.AsyncClient(headers=None) as client:
            data = await client.get(f"https://ortex-gui.ortex.com/interface/api/news/universe/7/list?GUIv=2&page_size={page_size}&page={page_number}&min_impact={min_impact}&size={size}&customFilterModel=%7B%22items%22:[]%7D")
            data = data.json()


            length = data.get('length')
            rows = data.get('rows')
            obfuscated = data.get('obfuscated')
            obfuscationReason = data.get('obfuscationReason')
            columns = data.get('columns')
            columnVisibilityModel = data.get('columnVisibilityModel')
            customFilterModel = data.get('customFilterModel')
            sortModel = data.get('sortModel')


            columns = NewsColumns(columns)
            rows = NewsRows(rows)

            return columns, rows