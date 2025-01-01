import sys
from pathlib import Path
import random
import json
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[3])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import pandas as pd
from ..._asyncpg.asyncpg_sdk import AsyncpgSDK
from ..._pandas.pandas_sdk import PandasSDK
import asyncio
class Entries(AsyncpgSDK, PandasSDK):
    def __init__(self, data, *kwargs):
        self.pool = None
        super().__init__(*kwargs)

   

        self.title = [i.get('title') for i in data]
        title_detail = [i.get('title_detail') for i in data]

        self.type = [i.get('type') for i in title_detail]
        self.base = [i.get('base') for i in title_detail]
        self.value = [i.get('value') for i in title_detail]

        links = [i.get('links') for i in data]
        flat_links = [item for sublist in links for item in sublist]
        self.href = [i.get('href') for i in flat_links]
        self.link = [i.get('link') for i in data]
        self.id = [i.get('id') for i in data]
        self.summary = [i.get('summary') for i in data]
        summary_detail = [i.get('summary_detail') for i in data]
        self.summary_type = [i.get('type') for i in summary_detail]
        self.summary_base = [i.get('base') for i in summary_detail]
        self.summary_value = [i.get('value') for i in summary_detail]
        self.published = [i.get('published') for i in data]
        self.edgar_companyname = [i.get('edgar_companyname') for i in data]
        self.edgar_formtype = [i.get('edgar_formtype') for i in data]
        self.edgar_filingdate = [i.get('edgar_filingdate') for i in data]
        self.edgar_ciknumber = [i.get('edgar_ciknumber') for i in data]
        self.edgar_accessionnumber = [i.get('edgar_accessionnumber') for i in data]
        self.edgar_filenumber = [i.get('edgar_filenumber') for i in data]
        self.edgar_acceptancedatetime = [i.get('edgar_acceptancedatetime') for i in data]
        self.edgar_period = [i.get('edgar_period') for i in data]
        self.edgar_assistantdirector = [i.get('edgar_assistantdirector') for i in data]
        self.edgar_assignedsic = [i.get('edgar_assignedsic') for i in data]
        self.edgar_fiscalyearend = [i.get('edgar_fiscalyearend') for i in data]
        edgar_xbrlfile = [i.get('edgar_xbrlfile') for i in data]
        self.xrbl_sequence = [i.get('sequence') for i in edgar_xbrlfile]
        self.xrbl_file = [i.get('file') for i in edgar_xbrlfile]
        self.xrbl_type = [i.get('type') for i in edgar_xbrlfile]
        self.xrbl_size = [i.get('size') for i in edgar_xbrlfile]
        self.xrbl_description = [i.get('description') for i in edgar_xbrlfile]
        self.xrbl_url = [i.get('url') for i in edgar_xbrlfile]



        self.data_dict = { 
            'title': self.title,
            'type': self.type,
            'base': self.base,
            'value': self.value,
            'link': self.link,
            'id': self.id,
            'summary': self.summary,
            'summary_type': self.summary_type,
            'summary_base': self.summary_base,
            'summary_value': self.summary_value,
            'published': self.published,
            'company_name': self.edgar_companyname,
            'form_type': self.edgar_formtype,
            'filingdate': self.edgar_filingdate,
            'cik_number': self.edgar_ciknumber,
            'accession_number': self.edgar_accessionnumber,
            'file_number': self.edgar_filenumber,
            'acceptance_datetime': self.edgar_acceptancedatetime,
            'period': self.edgar_period,
            'assistant_director': self.edgar_assistantdirector,
            'assigned_sic': self.edgar_assignedsic,
            'fiscal_yearend': self.edgar_fiscalyearend,
            'xrbl_sequence': self.xrbl_sequence,
            'xrbl_file': self.xrbl_file,
            'xrbl_type': self.xrbl_type,
            'xrbl_size': self.xrbl_size,
            'xrbl_description': self.xrbl_description,
            'xrbl_url': self.xrbl_url,
        }

        self.print_lengths(self.data_dict)


        self.as_dataframe = pd.DataFrame(self.data_dict)



class DocumentParser:
    def __init__(self, data):

        self.seq = [i.get('seq') for i in data]
        self.description = [i.get('description') for i in data]
        self.document_link = [f"https://www.sec.gov{i.get('document_link')}" for i in data]
        self.type = [i.get('type') for i in data]
        self.size = [i.get('size') for i in data]



        self.data_dict = { 
            'seq': self.seq,
            'description': self.description,
            'document_link': self.document_link,
            'type': self.type,
            'size': self.size
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)



class FilerInfo:
    def __init__(self, data):
        self.mailing_address = data.get('mailing_address')
        self.business_address = data.get('business_address')
        self.company_name = data.get('company_name')
        self.cik = data.get('cik').split(' ')[0]


        self.data_dict = { 
            'mailing_address': self.mailing_address,
            'business_address': self.business_address,
            'company': self.company_name,
            'cik': self.cik
        }



        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])





class Submissions:
    def __init__(self, data):

        self.cik = data.get('cik')
        self.entityType = data.get('entityType')
        self.sic = data.get('sic')
        self.sicDescription = data.get('sicDescription')
        self.ownerOrg = data.get('ownerOrg')
        self.insiderTransactionForOwnerExists = data.get('insiderTransactionForOwnerExists')
        self.insiderTransactionForIssuerExists = data.get('insiderTransactionForIssuerExists')
        self.name = data.get('name')
        self.tickers = ','.join(data.get('tickers'))
        self.exchanges = ','.join(data.get('exchanges'))
        self.ein = data.get('ein')
        self.description = data.get('description')
        self.website = data.get('website')
        self.investorWebsite = data.get('investorWebsite')
        self.category = data.get('category')
        self.fiscalYearEnd = data.get('fiscalYearEnd')
        self.stateOfIncorporation = data.get('stateOfIncorporation')
        self.stateOfIncorporationDescription = data.get('stateOfIncorporationDescription')
        self.addresses = data.get('addresses')
        self.phone = data.get('phone')
        self.flags = data.get('flags')
        self.formerNames = data.get('formerNames')
        filings = data.get('filings')
        files = filings.get('files')
        self.files_name = [i.get('name') for i in files]

        self.data_dict = { 
            'cik': self.cik,
            'entityType': self.entityType,
            'sic': self.sic,
            'sicDescription': self.sicDescription,
            'ownerOrg': self.ownerOrg,
            'insiderTransactionForOwnerExists': self.insiderTransactionForOwnerExists,
            'insiderTransactionForIssuerExists': self.insiderTransactionForIssuerExists,
            'name': self.name,
            'tickers': self.tickers,
            'exchanges': self.exchanges,
            'ein': self.ein,
            'description': self.description,
            'website': self.website,
            'investorWebsite': self.investorWebsite,
            'category': self.category,
            'fiscalYearEnd': self.fiscalYearEnd,
            'stateOfIncorporation': self.stateOfIncorporation,
            'stateOfIncorporationDescription': self.stateOfIncorporationDescription,
            'phone': self.phone,
            'flags': self.flags,
            'files_name': self.files_name,
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)