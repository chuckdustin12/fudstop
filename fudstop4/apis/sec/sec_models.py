import pandas as pd




class RecentSubmissions:
    def __init__(self, recent, ticker):


        self.accessionNumber = recent.get('accessionNumber')
        self.filingDate = recent.get('filingDate')
        self.reportDate = recent.get('reportDate')
        self.acceptanceDateTime = recent.get('acceptanceDateTime')
        self.act = recent.get('act')
        self.form = recent.get('form')
        self.fileNumber = recent.get('fileNumber')
        self.filmNumber = recent.get('filmNumber')
        self.items = recent.get('items')
  
        self.core_type = recent.get('core_type')
        self.size = recent.get('size')
        self.isXBRL = recent.get('isXBRL')
        self.isInlineXBRL = recent.get('isInlineXBRL')
        self.primaryDocument = recent.get('primaryDocument')
        self.primaryDocDescription = recent.get('primaryDocDescription')


        self.data_dict = { 
            'accession_number': self.accessionNumber,
            'file_date': self.filingDate,
            'report_date': self.reportDate,
            'acceptance_time': self.acceptanceDateTime,
            'act': self.act,
            'form': self.form,
            'file_number': self.fileNumber,
            'film_number': self.filmNumber,
            'core_type': self.core_type,
            'size': self.size,
            'primary_doc': self.primaryDocument,
            'primary_doc_desc': self.primaryDocDescription,
        
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)
        self.as_dataframe['ticker'] = ticker