import pandas as pd

import asyncio
import httpx
from .casetext_models import CaseLawBriefs,CaseLawCases,CaseLawRules
from fudstop.apis.polygonio.polygon_database import PolygonDatabase
import urllib.parse
import re


class CasetextSDK:
    def __init__(self):
        self.db = PolygonDatabase(database='law')
    # Function to pad lists to the same length
    # Function to pad lists to the same length
    def pad_list(self, lst, length, padding_value=''):
        return lst + [padding_value] * (length - len(lst))
    async def fetch_data(self, url, params=None):
        try:
            async with httpx.AsyncClient() as client:
                data = await client.get(url, params=params)
                if data.status_code == 200:
                    data = data.json()
                    return data
        except Exception as e:
            print(e)

    def clean_text(self, text):
        text = re.sub('<[^<]+?>', '', text)  # Remove HTML tags
        text = text.replace("'", "").replace("\\", "")  # Remove apostrophes and backslashes
        return text
    async def parallell_search_holdings(self, query, page: str = '1'):
        async with httpx.AsyncClient() as client:
            url = f"https://parallelsearch.casetext.com/__search/unified?q={query}&jxs=txsct,txapp&page={page}&sort=relevance&type=holding"
            print(f"Fetching URL: {url}")
            response = await client.get(url)
            data = response.json()

            results = data.get('results', {})
            holding = results.get('holding', {})
            rows = holding.get('rows', [])

            # Initialize lists for the dataframe columns
            slugs, citation_strings, cleaned_rows, total_count = [], [], [], []
            summarizing_slugs, summarizing_titles, summarizing_texts, summary_citations = [], [], [], []

            # Process each case (each row in the 'rows' field)
            for row in rows:
                # Extract slug, totalCount, rows, and citators
                slugs.append(row.get('slug', ''))
                total_count.append(row.get('totalCount', 0))

                # Process the 'rows' text and clean it
                row_texts = row.get('rows', [])
                combined_text = ' '.join(row_texts)
                cleaned_combined_text = self.clean_text(combined_text)
                cleaned_rows.append(cleaned_combined_text)

                # Extract summarizing cases
                summarizing_cases = row.get('summarizingCases', [])
                for case in summarizing_cases:
                    summarizing_slugs.append(case.get('id', ''))
                    summarizing_titles.append(case.get('title', ''))
                    summarizing_texts.append(self.clean_text(case.get('text', '')))
                    summary_citations.append(case.get('citationString', ''))

            # Find the maximum length
            max_length = max(len(slugs), len(cleaned_rows), len(total_count), len(summarizing_slugs), 
                            len(summarizing_titles), len(summarizing_texts), len(summary_citations))

            # Ensure all lists are the same length by padding the shorter lists
            slugs = self.pad_list(slugs, max_length)
            cleaned_rows = self.pad_list(cleaned_rows, max_length)
            total_count = self.pad_list(total_count, max_length, 0)
            summarizing_slugs = self.pad_list(summarizing_slugs, max_length)
            summarizing_titles = self.pad_list(summarizing_titles, max_length)
            summarizing_texts = self.pad_list(summarizing_texts, max_length)
            summary_citations = self.pad_list(summary_citations, max_length)

            # Create dictionary for DataFrame
            data_dict = { 
                'slug': slugs,
                'rows': cleaned_rows,
                'count': total_count,
                'summarizing_slug': summarizing_slugs,
                'summarizing_title': summarizing_titles,
                'summary_text': summarizing_texts,
                'summary_citation': summary_citations
            }

            # Create DataFrame
            df = pd.DataFrame(data_dict)


            return df



    async def parallel_search_cases(self, query, db=None, page:str='1', sort:str='relevance'):
        """Casetext"""
        # Remove URL encoding
        endpoint = f"https://parallelsearch.casetext.com/__search/unified?q={query}&jxs=txsct,txapp&page={page}&sort={sort}&type=case"
        print(endpoint)
        data = await self.fetch_data(endpoint)
        results = data['results']
        case = results['case']
        case_rows = case['rows']
        final = CaseLawCases(case_rows)
        
        if db is not None:
            await db.connect()
            await db.batch_insert_dataframe(final.as_dataframe, table_name='cases', unique_columns='slug')
        
        return final.as_dataframe
    async def parallel_search_briefs(self, query, db=None):
        encoded_query = urllib.parse.quote(query)
        endpoint = f"https://parallelsearch.casetext.com/__search/unified?q={encoded_query}&page=1&sort=relevance&type=brief"
        data = await self.fetch_data(endpoint)
        results = data['results']
        case = results['brief']
        case_rows = case['rows']
        final = CaseLawBriefs(case_rows)
        if db is not None:
            await db.connect()
            await db.batch_insert_dataframe(final.as_dataframe, table_name='briefs', unique_columns='slug')
        return final.as_dataframe

    async def parallel_search_rules(self, query, db=None):
        encoded_query = urllib.parse.quote(query)
        endpoint = f"https://parallelsearch.casetext.com/__search/unified?q={encoded_query}&page=1&sort=relevance&type=rule"
        data = await self.fetch_data(endpoint)
        results = data['results']
        case = results['rule']
        case_rows = case['rows']
        final = CaseLawRules(case_rows)
        if db is not None:
            await db.connect()
            await db.batch_insert_dataframe(final.as_dataframe, table_name='rules', unique_columns='slug')
        return final.as_dataframe

    async def parallel_search_statutes(self, query, db=None):
        encoded_query = urllib.parse.quote(query)
        endpoint = f"https://parallelsearch.casetext.com/__search/unified?q={encoded_query}&page=1&sort=relevance&type=statute"
        data = await self.fetch_data(endpoint)
        results = data['results']
        case = results['statute']
        case_rows = case['rows']
        final = CaseLawRules(case_rows)
        if db is not None:
            await db.connect()
            await db.batch_insert_dataframe(final.as_dataframe, table_name='statutes', unique_columns='slug')
        return final.as_dataframe


    async def bulk_search(self, query, type:str='case', state:str='tx',start:str='50'):
        url = f"https://casetext.com/api/search-api/search?count=50&start={start}&types={type}&aggregateJxTypes=all&countTypes=all&sort=relevance&q={query}&jxs={state}sct,{state}app"
        print(url)
        async with httpx.AsyncClient() as client:
            try:
                await asyncio.sleep(1.5)
                data = await client.get(url)
                await asyncio.sleep(1.5)
                if data.status_code == 200:
                    data = data.json()
                    print(data)

                    if type == 'case':
                        case = data.get(f"case")
                        rows = case.get('rows')
                        return CaseLawCases(rows)
                    if type == 'brief':
                        case = data.get(f"brief")
                        rows = case.get('rows')
                        return CaseLawBriefs(rows)
                    
                    if type == 'rule':
                        case = data.get(f"rule")
                        rows = case.get('rows')
                        return CaseLawRules(rows)
            except Exception as e:
                await asyncio.sleep(5)



        