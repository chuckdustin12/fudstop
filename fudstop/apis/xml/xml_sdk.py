import httpx
import pandas as pd
import xml.etree.ElementTree as ET
import requests
from fudstop.apis.polygonio.polygon_database import PolygonDatabase
from .xml_models.xml_sec import UsGaapFilings, TickerFilings, TypeFilings
from fudstop.apis.polygonio.async_polygon_sdk import Polygon
import re


class xmlSDK:
    def __init__(self):
        self.db = PolygonDatabase()
        self.poly = Polygon()
        self.inline_filings_url=f"https://www.sec.gov/Archives/edgar/usgaap.rss.xml"
        self.headers = { 
            'User-Agent': 'fudstop AdminContact@fudstop.io',
            "Accept-Encoding": "gzip, deflate",
            'Host': 'www.sec.gov'
        }
        self.base_url = f"https://www.sec.gov"

        self.ticker_df = pd.read_csv('files/ciks.csv')

    async def parse_us_gaap(self, insert:bool):
        if insert == True:
            await self.db.connect()
        # Fetch the RSS XML data from the URL
        async with httpx.AsyncClient(headers=self.headers) as client:
            response = await client.get("https://www.sec.gov/Archives/edgar/usgaap.rss.xml")
            response.raise_for_status()  # Ensure we got a successful response
            root = ET.fromstring(response.content)

            # Find all <item> elements
            items = root.findall('.//channel/item')
            results = []

            for item in items:
                parsed_item = {
                    'title': item.find('title').text if item.find('title') is not None else None,
                    'link': item.find('link').text if item.find('link') is not None else None,
                    'description': item.find('description').text if item.find('description') is not None else None,
                    'publication_date': item.find('pubDate').text if item.find('pubDate') is not None else None,
                    'files': []
                }

                # Extract XBRL filing information
                xbrl_filing = item.find('.//edgar:xbrlFiling', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'})
                if xbrl_filing is not None:
                    parsed_item.update({
                        'company_name': xbrl_filing.find('edgar:companyName', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}).text if xbrl_filing.find('edgar:companyName', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}) is not None else None,
                        'form_type': xbrl_filing.find('edgar:formType', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}).text if xbrl_filing.find('edgar:formType', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}) is not None else None,
                        'filing_date': xbrl_filing.find('edgar:filingDate', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}).text if xbrl_filing.find('edgar:filingDate', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}) is not None else None,
                        'cik_number': xbrl_filing.find('edgar:cikNumber', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}).text if xbrl_filing.find('edgar:cikNumber', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}) is not None else None,
                        'accession_number': xbrl_filing.find('edgar:accessionNumber', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}).text if xbrl_filing.find('edgar:accessionNumber', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}) is not None else None,
                        'file_number': xbrl_filing.find('edgar:fileNumber', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}).text if xbrl_filing.find('edgar:fileNumber', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}) is not None else None,
                        'acceptance_datetime': xbrl_filing.find('edgar:acceptanceDatetime', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}).text if xbrl_filing.find('edgar:acceptanceDatetime', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}) is not None else None,
                        'period': xbrl_filing.find('edgar:period', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}).text if xbrl_filing.find('edgar:period', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}) is not None else None,
                        'assistant_director': xbrl_filing.find('edgar:assistantDirector', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}).text if xbrl_filing.find('edgar:assistantDirector', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}) is not None else None,
                        'assigned_sic': xbrl_filing.find('edgar:assignedSic', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}).text if xbrl_filing.find('edgar:assignedSic', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}) is not None else None,
                        'fiscal_year_end': xbrl_filing.find('edgar:fiscalYearEnd', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}).text if xbrl_filing.find('edgar:fiscalYearEnd', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'}) is not None else None,
                    })

                    # Extract XBRL file details
                    xbrl_files = xbrl_filing.findall('edgar:xbrlFiles/edgar:xbrlFile', namespaces={'edgar': 'https://www.sec.gov/Archives/edgar'})
                    for xbrl_file in xbrl_files:
                        file_info = {
                            'sequence': xbrl_file.attrib.get('{https://www.sec.gov/Archives/edgar}sequence', None),
                            'file': xbrl_file.attrib.get('{https://www.sec.gov/Archives/edgar}file', None),
                            'type': xbrl_file.attrib.get('{https://www.sec.gov/Archives/edgar}type', None),
                            'size': xbrl_file.attrib.get('{https://www.sec.gov/Archives/edgar}size', None),
                            'description': xbrl_file.attrib.get('{https://www.sec.gov/Archives/edgar}description', None),
                            'url': xbrl_file.attrib.get('{https://www.sec.gov/Archives/edgar}url', None),
                            'inlineXBRL': xbrl_file.attrib.get('{https://www.sec.gov/Archives/edgar}inlineXBRL', 'false')
                        }
                        parsed_item['files'].append(file_info)
                
                results.append(parsed_item)
            await self.db.batch_insert_dataframe(UsGaapFilings(results).as_dataframe, table_name='sec_filings', unique_columns='ticker, publication_date')
            return UsGaapFilings(results)



    async def parse_filings_by_ticker(self, ticker, count='40', insert:bool=False):
        if insert == True:
            await self.db.connect()
        # Placeholder for the CIK fetching logic
        cik = await self.poly.get_cik(ticker)  # Assuming you have this function implemented

        endpoint = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK={cik}&count={count}&output=atom"

        async with httpx.AsyncClient(headers=self.headers) as client:
            response = await client.get(endpoint)
            root = ET.fromstring(response.content)

            namespaces = {'atom': 'http://www.w3.org/2005/Atom'}
            entries = root.findall('atom:entry', namespaces)

            results = []
            for entry in entries:
                title = entry.find('atom:title', namespaces).text
                link = entry.find('atom:link', namespaces).attrib['href']
                summary = entry.find('atom:summary', namespaces).text
                updated = entry.find('atom:updated', namespaces).text
                form_type = entry.find('atom:category', namespaces).attrib['term']
                acc_no = entry.find('atom:id', namespaces).text.split('accession-number=')[-1]

                results.append({
                    'title': title,
                    'link': link,
                    'summary': summary,
                    'updated': updated,
                    'form_type': form_type,
                    'accession_number': acc_no
                })
            try:
                await self.db.batch_insert_dataframe(TickerFilings(results, ticker).as_dataframe, table_name='sec_ticker', unique_columns='ticker, link')
            except Exception as e:
                print(f"ERROR! Use insert=True! as an input argument!")
                print(e)
            return TickerFilings(results, ticker)
        

    async def parse_filings_by_type(self, type:str='13F', count:str='40', insert:bool=False):
        if insert == True:
            await self.db.connect()

        endpoint = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={type}&owner=include&start=0&count={count}&output=atom"

        async with httpx.AsyncClient(headers=self.headers) as client:
            response = await client.get(endpoint)
            root = ET.fromstring(response.content)

            namespaces = {'atom': 'http://www.w3.org/2005/Atom'}
            entries = root.findall('atom:entry', namespaces)

            results = []
            for entry in entries:
                title = entry.find('atom:title', namespaces).text
                link = entry.find('atom:link', namespaces).attrib['href']
                summary = entry.find('atom:summary', namespaces).text
                updated = entry.find('atom:updated', namespaces).text
                form_type = entry.find('atom:category', namespaces).attrib['term']
                acc_no = entry.find('atom:id', namespaces).text.split('accession-number=')[-1]

                results.append({
                    'title': title,
                    'link': link,
                    'summary': summary,
                    'updated': updated,
                    'form_type': form_type,
                    'accession_number': acc_no
                })
            try:
                await self.db.batch_insert_dataframe(TypeFilings(results).as_dataframe, table_name='sec_type', unique_columns='ticker, form_type')
            except Exception as e:
                print(f"ERROR! Use insert=True! as an input argument!")
                print(e)
            return TickerFilings(results, type)