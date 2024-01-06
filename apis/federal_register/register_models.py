import os

import pandas as pd

class DocumentQuery:
    def __init__(self, data):

        try:
            self.count = data.get('count')
            self.description = data.get('description')
            self.total_pages = data.get('total_pages')
            results = data.get('results')

            self.title = [i.get('title', 'N/A') for i in results]
            self.type = [i.get('type', 'N/A') for i in results]
            self.abstract = [i.get('abstract', 'N/A') for i in results]
            self.document_number = [i.get('document_number', 'N/A') for i in results]
            self.html_url= [i.get('html_url', 'N/A') for i in results]
            self.pdf_url= [i.get('pdf_url', 'N/A') for i in results]
            self.public_inspection_pdf_url= [i.get('public_inspection_pdf_url', 'N/A') for i in results]
            self.publication_date= [i.get('publication_date', 'N/A') for i in results]
            agencies= [i.get('agencies', 'N/A') for i in results]
            # Assuming agencies[0][0] is a dictionary
            attributes = ['raw_name', 'name', 'id', 'url', 'json_url', 'parent_id', 'slug']
            extracted_attributes = [{attr: agency[0].get(attr, 'N/A') for attr in attributes} for agency in agencies]
            # Extract only the 'name' attribute from each agency dictionary
            self.agency_names = [agency['name'] for agency in extracted_attributes]

            self.excerpts= [i.get('excerpts', 'N/A') for i in results]
            print(extracted_attributes)
    


            self.data_dict = { 

                'result_count': self.count,
                'search_description': self.description,
                'total_pages': self.total_pages,
                'title': self.title,
                'type': self.type,
                'abstract': self.abstract,
                'document_number': self.document_number,
                'html_url': self.html_url,
                'pdf_url': self.pdf_url,
                'public_inspection_url': self.public_inspection_pdf_url,
                'publication_date': self.publication_date,
                'excerpts': self.excerpts,
                'agency': self.agency_names

            }
        

            self.as_dataframe = pd.DataFrame(self.data_dict)

            # Create the directory if it doesn't exist
            directory = 'data/fed_register/'
            if not os.path.exists(directory):
                os.makedirs(directory)


            # Create the initial DataFrame
            self.as_dataframe = pd.DataFrame(self.data_dict)

            # Create directory if it doesn't exist
            directory = 'data/fed_register/'
            if not os.path.exists(directory):
                os.makedirs(directory)

            # Save DataFrame to CSV
            self.as_dataframe.to_csv('data/fed_register/query_results.csv')
        except Exception as e:
            return e
