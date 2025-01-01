import pandas as pd
import re
from fudstop.apis.polygonio.async_polygon_sdk import Polygon
class UsGaapFilings: #https://www.sec.gov/Archives/edgar/usgaap.rss.xml
    def __init__(self, data):


        self.title = [i.get('title') for i in data]
        self.link = [i.get('link') for i in data]
        self.description = [i.get('description') for i in data]
        self.publication_date = [i.get('publication_date') for i in data]
        self.files = [i.get('files') for i in data]
        self.company_name = [i.get('company_name') for i in data]
        self.form_type = [i.get('form_type') for i in data]
        self.filing_date = [i.get('filing_date') for i in data]
        self.cik_number = [i.get('cik_number') for i in data]
        self.accession_number = [i.get('accession_number') for i in data]
        self.file_number = [i.get('file_number') for i in data]
        self.acceptance_datetime = [i.get('acceptance_datetime') for i in data]
        self.period = [i.get('period') for i in data]
        self.assistant_director = [i.get('assistant_director') for i in data]
        self.assigned_sic = [i.get('assigned_sic') for i in data]
        self.fiscal_year_end = [i.get('fiscal_year_end') for i in data]

        self.data_dict = {
            'title': [i.get('title') for i in data],
            'link': [i.get('link') for i in data],
            'description': [i.get('description') for i in data],
            'publication_date': [i.get('publication_date') for i in data],
            'files': [i.get('files') for i in data],
            'company_name': [i.get('company_name') for i in data],
            'form_type': [i.get('form_type') for i in data],
            'filing_date': [i.get('filing_date') for i in data],
            'cik_number': [i.get('cik_number') for i in data],
            'accession_number': [i.get('accession_number') for i in data],
            'file_number': [i.get('file_number') for i in data],
            'acceptance_datetime': [i.get('acceptance_datetime') for i in data],
            'period': [i.get('period') for i in data],
            'assistant_director': [i.get('assistant_director') for i in data],
            'assigned_sic': [i.get('assigned_sic') for i in data],
            'fiscal_year_end': [i.get('fiscal_year_end') for i in data],
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)

                # Load CIK to ticker mapping
        self.cik_mapping = pd.read_csv('files/ciks.csv', dtype={'cik': str})
        
        # Merge the data with the CIK to ticker mapping
        self.as_dataframe = self.as_dataframe.merge(self.cik_mapping, how='left', left_on='cik_number', right_on='cik')
        self.as_dataframe.drop(columns=['cik'], inplace=True)  # Drop the duplicate CIK column if needed



class TickerFilings:
    def __init__(self, data, ticker):

        self.title = [i.get('title') for i in data]
        self.link = [i.get('link') for i in data]
        self.summary = [i.get('summary') for i in data]
        self.updated = [i.get('updated') for i in data]
        self.form_type = [i.get('form_type') for i in data]
        self.accession_number = [i.get('accession_number') for i in data]


        self.data_dict = { 
            'ticker': ticker,
            'title': self.title,
            'link': self.link,
            'updated': self.updated,
            'form_type': self.form_type,
            'accession_number': self.accession_number
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)


class TypeFilings:
    def __init__(self, data):
        self.title = [i.get('title') for i in data]
        self.link = [i.get('link') for i in data]
        self.summary = [i.get('summary') for i in data]
        self.updated = [i.get('updated') for i in data]
        self.form_type = [i.get('form_type') for i in data]
        self.accession_number = [i.get('accession_number') for i in data]
        self.cik_number = [self.extract_cik_number(title) for title in self.title]

        self.data_dict = { 
            'title': self.title,
            'link': self.link,
            'summary': self.summary,
            'updated': self.updated,
            'form_type': self.form_type,
            'accession_number': self.accession_number,
            'cik_number': self.cik_number
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)
        # Load CIK to ticker mapping
        cik_mapping = pd.read_csv('files/ciks.csv', dtype={'cik': str})
        cik_mapping.columns = ['ticker', 'cik']  # Ensure the correct column names
        
        # Merge with the CIK to ticker mapping DataFrame
        self.filings_df = self.as_dataframe.merge(cik_mapping, how='left', left_on='cik_number', right_on='cik')
        self.filings_df.drop(columns=['cik'], inplace=True)  # Drop the duplicate CIK column if needed
        # Ensure the ticker column is included in the data_dict and as_dataframe
        self.data_dict['ticker'] = self.filings_df['ticker'].tolist()
        self.as_dataframe = pd.DataFrame(self.data_dict)

    @staticmethod
    def extract_cik_number(title):
        cik_match = re.search(r'\((\d{10})\)', title)
        return cik_match.group(1) if cik_match else None
