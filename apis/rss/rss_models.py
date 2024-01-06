import pandas as pd
import os

class Feed:
    def __init__(self, feed):

        self.entries = feed.get('entries', None)
        self.bozo = feed.get('bozo', None)
        self.headers = feed.get('headers', None)
        self.href = feed.get('href', None)
        self.status = feed.get('status', None)
        self.encoding = feed.get('encoding', None)
        self.version = feed.get('version', None)
        self.namespaces = feed.get('namespaces', None)



class Entries:
    def __init__(self, entries):
 
        self.terms = [tag['term'] for entry in entries for tag in entry.get('tags', [])]
        self.terms = [tag['scheme'] for entry in entries for tag in entry.get('tags', [])]
        self.filing_dates = [entry['filing-date'] for entry in entries if 'filing-date' in entry]
        self.links = [entry['link'] for entry in entries if 'link' in entry]
        self.form_names = [entry['form-name'] for entry in entries if 'form-name' in entry]


        self.data_dict = { 

            'term': self.terms,
            'filing_date': self.filing_dates,
            'report_url': self.links,
            'form_name': self.form_names
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)


        # Create directory if it doesn't exist
        directory = 'fudstop/data/rss_feeds/'
        if not os.path.exists(directory):
            os.makedirs(directory)



        self.as_dataframe.to_csv('fudstop/data/rss_feeds/company_filings.csv', index=False)
      





