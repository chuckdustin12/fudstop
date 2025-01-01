import sys
from pathlib import Path
import os
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import re
from _markets.list_sets.ticker_lists import most_active_tickers
from .rss_models import Feed,Entries
import feedparser
import requests
from ..polygonio.async_polygon_sdk import Polygon
import pandas as pd
import requests
import time
import aiohttp
import xml.etree.ElementTree as ET
from .rss_helpers import fix_xml_attributes, xml_to_dataframe,extract_xml_data


from bs4 import BeautifulSoup



most_active_tickers = set(most_active_tickers)

polygon=Polygon(host='localhost', user='chuck', database='market_data', password='fud', port=5432)


from datetime import datetime, timedelta

class SyncRSSSDK:
    def __init__(self):
        self.sec_base = "https://www.sec.gov"
        self.session = requests.session()
        self.headers = {
            "Accept": "application/json",
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
            }
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')

    

    def fetch_new_posts(self,url):
        last_update_time = None

        while True:

         
            feed_data = self.session.get(url).json()

            for post in feed_data:
                post_time = post['date']
                if post_time != last_update_time:
                    last_update_time = post_time
                    return post

            time.sleep(60)  # Wait for 60 seconds before fetching the feed again



    def company_filings(self, ticker, as_dataframe:bool=False, limit:str='100'):
        """
        Takes in a ticker - converts to CIK - passes to SEC Rss feed
        
        Arguments:

        >>> as_dataframe: optional - returns a dataframe of company filings


        >>> limit: optional - the number of filings to search (default 100)

        """

        cik = polygon.company_info_sync(ticker)
        if cik is not None:
            cik = cik.cik
            rss_url = f"https://data.sec.gov/rss?cik={cik}&count={limit}"
            print(rss_url)

            # Parse the RSS feed
            feed = feedparser.parse(rss_url, request_headers=self.headers)

            data = Feed(feed)
            
            entries = data.entries
            

            entries = Entries(entries)
        
            if as_dataframe == True:
                
                entries.as_dataframe.to_csv('data/rss_feeds/commpany_filings.csv')
                return entries.as_dataframe
            # Filter the links based on the dates
            links_today = [link for link, date in zip(entries.links, entries.filing_dates) if date == self.today]

        
            return links_today

    
    def scan_filing_type(self, ticker:str, type:str='4'):
        """
        Returns SEC index.htm for a specific filing type.

        Arguments:


        >>> ticker: REQUIRED - the ticker to query

        >>> type: optional - the type of filing to query (default 4)

            4 - form 4 
            10-Q 
            10-K
        """
        cik = polygon.company_info(ticker)
        if cik is not None:
            cik = cik.cik
            rss_url = f"https://data.sec.gov/rss?cik={cik}&count=100&type={type}"
    
            print(rss_url)

            # Parse the RSS feed
            feed = feedparser.parse(rss_url, request_headers=self.headers)

            data = Feed(feed)
            
            entries = data.entries
            

            entries = Entries(entries)
    
            # Filter the links based on the dates
            links_today = [link for link, date in zip(entries.links, entries.filing_dates) if date >= self.today]

        
            return links_today
    def get_filing_urls(self, ticker:str):
        try:
            filings = self.company_filings(ticker)
        except aiohttp.client.ClientConnectorError:
            print(f'Error - Ticker')

        if filings is not None:
       
            for filing in filings:
                response = requests.get(filing, headers=self.headers)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    table = soup.find('table', class_='tableFile')
                    
                    if table:
                        for row in table.find_all('tr')[1:]:  # Skip the header row
                            cells = row.find_all('td')
                            description = cells[1].text.strip()
                            document_link = cells[2].a['href'].strip() if cells[2].a else ""


                            #form 4 
                            if "xslF" not in document_link and document_link.endswith('.xml') and 'xsl144' not in document_link and 'primary_doc' not in document_link:
                                # Process the document link
                                print(description, document_link)
                                
                                xml_url = self.sec_base + document_link
                                response = requests.get(xml_url, headers=self.headers)
                                
                                if response.status_code == 200:
                                    xml_content = response.text if not response.text.startswith('<!DOCTYPE html') else None
                                    if xml_content is not None:
                                        
                                        file_name = 'company_filing_form4.xml'  # Get the last part of the URL as the file name
                                        
                                        with open(file_name, 'w', encoding='utf-8') as file:
                                            file.write(xml_content)
                                        print(f"XML file saved as {file_name}")
                                        # Parse the XML file
                                        tree = ET.parse(file_name)
                                        root = tree.getroot()
                                        
                                        df = xml_to_dataframe(file_name)
                                        df['ticker'] = ticker
                                        data_dict = df.to_dict(orient='dict')
                                        for key, value in data_dict.items():
                                            data_dict[key] = value[0]


                                        df = pd.DataFrame(data_dict, index=df['ticker'])

                                        print(df.transpose())
                                       
                                                                                    
                            if "primary_doc" in document_link and 'xslF' not in document_link and document_link.endswith('.xml'):
                                # Process the document link for xsl144 filings
                                print(description, document_link)
                                
                                xml_url = self.sec_base + document_link
                                response = requests.get(xml_url, headers=self.headers)
                                
                                if response.status_code == 200:
                                    xml_content = response.text if not response.text.startswith('<!DOCTYPE html') else None
                                    if xml_content is not None:
                                        
                                        file_name = 'company_filing_144.xml'  # Define a specific name for xsl144 filings
                                        
                                        with open(file_name, 'w', encoding='utf-8') as file:
                                            file.write(xml_content)
                                        print(f"XML file saved as {file_name}")
                                        
                                        # Parse the XML file
                                        tree = ET.parse(file_name)
                                        root = tree.getroot()
                                        
                                        df = xml_to_dataframe(file_name)
                                        df['ticker'] = ticker
                                        data_dict = df.to_dict(orient='dict')
                                        for key, value in data_dict.items():
                                            data_dict[key] = value[0]
                                        
                                        df = pd.DataFrame(data_dict, index=[ticker])
                                        # Remove namespaces from column names
                                        df.columns = [col.split('}')[-1] if '}' in col else col for col in df.columns]

                                

                                        # Print the cleaned DataFrame
                                        print(df.transpose())          
                                        return df                          

                        else:
                            if document_link.endswith('xml'):
                                print('https://www.sec.gob/'+document_link)
                            



