import asyncio
import aiohttp
import requests
session = requests.session()
from datetime import datetime, timedelta

from .register_models import DocumentQuery


class FedRegisterSDK:
    def __init__(self):


        
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')

    def get_document(self, document_number:str):
        """
        Returns a document from the Federal Register


        Arguments:


        >>> document_number: the document number to query
        
        """

        endpoint = f"https://www.federalregister.gov/api/v1/documents/{document_number}.json"

        data = session.get(endpoint).json()
        print(data)
        return data



    def query_document(self, text:str, effective_date_greater_than:str='2019-09-17', effective_date_less_than:str=None, order:str='newest', is_significant:str='1'):
        """
        Query a Federal Register document based on textual search and effectiveness
        as well as whether or not the document is "significatn".

        ARGUMENTS:

        >>> text search: the text to search for (REQUIRED)


        >>> order: the order of the returned items: (optional)
           
           - newest (default)
           - oldest 
           - relevance
           - executive_order_number

           
        >>> effective_date_greater_than: the date to begin surveying (optional - default 2019-09-17)


        >>> effective_date_less_than: the date to end surveying (optional - default today)


        >>> is_significant: whether the document is significant or not. (optional - default 1)

          - 1 = TRUE

          - 0 = FALSE

        """

        if effective_date_less_than is None:
            effective_date_less_than = self.today



        endpoint = f"https://www.federalregister.gov/api/v1/documents.json?per_page=10&page=1&order={order} \
        &conditions[term]={text}&conditions[effective_date][gte]={effective_date_greater_than}&conditions[effective_date][lte]={effective_date_less_than} \
        &conditions[agencies][]=securities-and-exchange-commission&conditions[significant]={is_significant}"

        data = session.get(endpoint)
        data =  data.json()
        return DocumentQuery(data)
