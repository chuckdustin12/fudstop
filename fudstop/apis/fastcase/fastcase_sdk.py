import asyncio
import httpx
from fudstop.apis.polygonio.polygon_database import PolygonDatabase
import pandas as pd
from .fastcase_models.fastcase_models import DocumentDetails

import re
from bs4 import BeautifulSoup

def clean_html(html):
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Remove script and style elements
    for script_or_style in soup(['script', 'style']):
        script_or_style.decompose()

    # Get text and remove extra whitespace
    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)

    # Remove page numbers
    text = re.sub(r'Page \d+', '', text)

    # Remove other unwanted patterns
    text = re.sub(r'\r\n', '', text)  # Remove \r\n
    text = re.sub(r'\x0c', '', text)  # Remove \x0c
    text = re.sub(r'&#160;', ' ', text)  # Replace HTML space entities with a space

    # Remove footnote references and any remaining unwanted tags
    text = re.sub(r'<SMALL><SUP>\d+</SUP></SMALL>', '', text)
    text = re.sub(r'<.*?>', '', text)

    return text
class FastcaseSDK:
    def __init__(self, cookie):
        self.cookie = { 'cookie': cookie
        }


        self.db = PolygonDatabase(user='chuck', database='law', password='fud', port=5432, host='localhost')




    
    # async def test(self):
    #     await self.db.connect()
    #     payload = {}
    #     endpoint = f"https://fc7-fastcase-com.ezproxy.sll.texas.gov:2443/searchApi/lookup/document/documentdetails/{universal_id}"

    #     async with httpx.AsyncClient(verify=False) as client:
    #         data = await client.post(endpoint, json=payload, headers=headers)



    async def term_lookup(self, term:str='abuse'):
        endpoint = f"https://fc7-fastcase-com.ezproxy.sll.texas.gov:2443/searchApi/typeahead/terms/{term}"


        async with httpx.AsyncClient(headers=self.cookie,verify=False) as client:

            data = await client.get(endpoint)

            data = data.json()

            for i in data:
                print(i)


    async def document_lookup(self, document:str='abuse'):
        endpoint = f"https://fc7-fastcase-com.ezproxy.sll.texas.gov:2443/searchApi/typeahead/documents/{document}"
        print(endpoint)
        await self.db.connect()


        async with httpx.AsyncClient(headers=self.cookie,verify=False) as client:

            data = await client.get(endpoint)

            data = data.json()
            print(data)
            self.universal_id = [i.get('universalId') for i in data]
            name = [i.get('title') for i in data]

            dict = { 
                'id': self.universal_id,
                'name': name,

            }
            df = pd.DataFrame(dict)
            await self.db.batch_insert_dataframe(df, table_name='law_documents', unique_columns='id')
            return self.universal_id

    async def get_case_ids(self, query:str):
        payload={"q":f"\"{query}","order":"desc","jdxType":[{"type":"Cases","jdx":["TX"]}],"selectedJurisdictions":["TX"],"sortBy":1,"skip":0,"ignoreRegex":False}
        endpoint = f"https://fc7-fastcase-com.ezproxy.sll.texas.gov:2443/searchApi/search/results"
        async with httpx.AsyncClient(headers=self.cookie, verify=False) as client:
            data = await client.post(endpoint, data=payload)

            print(data.json())


    async def get_cases(self, query):
        counter = 0
        await self.db.connect()
        query = f"\"{query}\""
        endpoint = f"https://fc7-fastcase-com.ezproxy.sll.texas.gov:2443/searchApi/search/results"
        dfs = []
        
        while True:
            counter = counter + 20
            payload = {"q":query,"order":"desc","jdxLibraries":"[{\"jdx\":\"TX\",\"libraries\":[128,160,15,283]}]","selectedJurisdictions":["TX"],"sortBy":1,"skip":counter,"ignoreRegex":True}
            async with httpx.AsyncClient(headers=self.cookie, verify=False) as client:
                data = await client.post(endpoint, data=payload)
                
                response = data.json()
                print(data)
                cited_max = response.get('citedGenerallyMax', None)
                                    



                documents = response.get('documents', [])

                authorityLevel = [i.get('authorityLevel') for i in documents]
                authority_level = [i.get('name') for i in authorityLevel]
                citedGenerally = [i.get('citedGenerally', 0) for i in documents]
                citedHere = [i.get('citedHere') for i in documents]
                date = [i.get('date') for i in documents]
                importDate = [i.get('importDate') for i in documents]
                fullCitation = [i.get('fullCitation') for i in documents]
                identifyingCitations = ', '.join(['; '.join(doc.get('identifyingCitations', [])) for doc in documents])


                jurisdiction = [i.get('jurisdiction') for i in documents]
                libraryType = [i.get('libraryType') for i in documents]
                mostRelevantParagraph = [i.get('mostRelevantParagraph') for i in documents]
                relevance = [i.get('relevance') for i in documents]
                shortName = [i.get('shortName') for i in documents]
                universalFilter = [i.get('universalFilter') for i in documents]
                universalId = [i.get('universalId') for i in documents]
                isbadLaw = ['1' if i.get('isbadLaw') else '0' for i in documents]
                aggregateTreatmentId = [i.get('aggregateTreatmentId') for i in documents]
                showOutline = [i.get('showOutline') for i in documents]
                canBuy = [i.get('canBuy') for i in documents]


                data_dict = { 
                    'date': date,
                    'authority': authority_level,
                    'cited_generally': citedGenerally,
                    'citation': fullCitation,
                    'identifying_citations': identifyingCitations,
                    'short_name': shortName,
                    'relevant_paragraph': mostRelevantParagraph,
                    'is_bad': isbadLaw,
                    'universal_id': universalId,
                    'query': query

                }
        
                df = pd.DataFrame(data_dict)
                print(df)
            
            
        
                df['cited_generally'] = pd.to_numeric(df['cited_generally'], errors='coerce')
                filtered_df = df.loc[(df['is_bad'] == '0') & (df['cited_generally'] > 100)]
                sorted_df = filtered_df.sort_values('cited_generally', ascending=False)

                await self.db.batch_insert_dataframe(sorted_df, table_name='caselaw', unique_columns='universal_id')
                if counter == 2000:
                    break



    async def documents(self, universal_id:str='7694152', input:str='abuse'):

        query = f"""SELECT universal_id FROM caselaw where relevant_paragraph ILIKE '%{input}%'"""


        results = await self.db.fetch(query)


        df = pd.DataFrame(results, columns=['universal_id'])


        print(df)


    async def document_details(self, universal_id:str, db:bool):
  
        endpoint = f"https://fc7-fastcase-com.ezproxy.sll.texas.gov:2443/searchApi/lookup/document/documentdetails/{universal_id}"
        print(endpoint)
        async with httpx.AsyncClient(headers=self.cookie2, verify=False) as client:
            data = await client.get(endpoint)

            data = data.json()
            print(data)
            data= DocumentDetails(data)
            if db == True:
                await db.batch_insert_dataframe(data.as_dataframe, table_name='document_details', unique_columns='universal_id')

            return data
    async def get_full_citation(self, universal_id, input:str, db=None):
        endpoint = f"https://fc7-fastcase-com.ezproxy.sll.texas.gov:2443/document/getDocumentWithFullCitation"
        payload={"UniversalId":f"{universal_id}","SearchPhrase":f"{input}"}
        async with httpx.AsyncClient(headers=self.cookie2, verify=False) as client:
            data = await client.post(endpoint, data=payload)

            data = data.json()

            html = data.get('DocumentHtml')
            citation = data.get('FullCitation')
            id = data.get('UniversalId')

            dict = { 
                'id': id,
                'search_phrase': input,
                'case_story': html,
                'citation': citation,
            }

            df = pd.DataFrame(dict, index=[0])
            if db is not None:
                await db.batch_insert_dataframe(df, table_name='case_study', unique_columns='id')
            return clean_html(html)
            
