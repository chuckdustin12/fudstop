from imps import *

import os
from dotenv import load_dotenv
load_dotenv()
import requests
case_id = os.environ.get('divorce_case_id')


class ResearchTexasSDK:
    def __init__(self):
        pass


    async def get_filings(self, id: str, page_size: int = 50, search_text: str = '', headers: str = None, index=0):
        """Get case filings"""
        url = f"https://research.txcourts.gov/CourtRecordsSearch/case/{id}/events"
        
        r = requests.post(url=url, data={"pageSize": page_size, "pageIndex": index, "sortNewestToOldest": False, 
                                         "searchText": search_text, "isSearchAll": True, "eventType": 0}, 
                                         headers=headers)
        
        if r.status_code == 200:
            r = r.json()
            events = r.get('events', [])

            # Extracting data from the events
            filing_code = [i.get('filingCode') for i in events]
            filing_date = [i.get('submitted') for i in events]
            file_size = [i.get('fileSize') for i in events]
            description = [i.get('description') for i in events]
            submitted = [i.get('submitted') for i in events]
            submitter_full_name = [i.get('submitterFullName') for i in events]
            docketed = [i.get('docketed') for i in events]
            filing_id = [i.get('filingId') for i in events]

            # Creating a dictionary
            data_dict = {
                'filing_id': filing_id,
                'filing_code': filing_code,
                'file_size': file_size,
                'description': description,
                'submitted': submitted,
                'submitter_full_name': submitter_full_name,
                'filing_date': filing_date,
                'docketed': docketed,
            }




    async def get_events(self, headers, pageindex):
        """Get case events"""
        url = f"https://research.txcourts.gov/CourtRecordsSearch/case/b04183e55b355b628fa3d90671422f9b/events"
        payload = {"pageSize": 50, "pageIndex": pageindex, "sortNewestToOldest": False, "searchText": None, "isSearchAll": True, "eventType": 0}
        
        r = requests.post(url, headers=headers, json=payload)
        if r.status_code == 200:
            r = r.json()
            events = r.get('events', [])

            docketed = [i.get('docketed') for i in events]
            submitter = [i.get('submitterFullName') for i in events]
            documents = [i.get('documents', []) for i in events]

            # Flatten the documents
            flat_docs = [item for sublist in documents for item in sublist]
            
            # Extract fields from flat_docs
            doc_description = [i.get('description') for i in flat_docs]
            doc_category = [i.get('documentCategoryCode') for i in flat_docs]
            filing_code = [i.get('filingCode') for i in flat_docs]
            page_count = [i.get('pageCount') for i in flat_docs]
            text_content = [i.get('textContent', '') for i in flat_docs]

            # Ensure consistent data by cleaning newlines or inconsistent text
            text_content = [text.replace('\n', ' ') if isinstance(text, str) else '' for text in text_content]

            # Truncate all lists to the minimum length if necessary
            min_length = min(len(submitter), len(doc_description), len(doc_category), len(filing_code), len(page_count), len(text_content), len(docketed))

            # Truncate all lists to the minimum length
            submitter = submitter[:min_length]
            doc_description = doc_description[:min_length]
            doc_category = doc_category[:min_length]
            filing_code = filing_code[:min_length]
            page_count = page_count[:min_length]
            text_content = text_content[:min_length]
            docketed = docketed[:min_length]

            # Create dictionary for DataFrame
            dict_data = {
                'submitter': submitter,
                'description': doc_description,
                'document_category': doc_category,
                'filing_code': filing_code,
                'pages': page_count,
                'text_content': text_content,
                'docketed': docketed,
            }

            # Create DataFrame
            df = pd.DataFrame(dict_data)

            return df