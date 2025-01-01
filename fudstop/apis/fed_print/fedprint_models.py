import pandas as pd

class Series:
    def __init__(self, data):
        self.format = data.get('format')
        self.page = data.get('page')
        self.limit = data.get('limit')
        self.number_of_results = data.get('number_of_results')
        records = data.get('records')

        self.id = [i.get('id') for i in records]
        # Initialize lists with default values "N/A"
        self.title = [i.get('title', 'N/A') for i in records]
        self.author = [i.get('author', 'N/A') for i in records]
        self.number = [i.get('number', 'N/A') for i in records]
        self.abstract = [i.get('abstract', 'N/A') for i in records]
        self.jel = [i.get('jel', 'N/A') for i in records]
        file = [i.get('file', 'N/A') for i in records]

        # Handle the file url separately to avoid issues with subscripting
        if file is not None:
            self.fileurl = [i[0].get('fileurl', 'N/A') if isinstance(i, list) and i and i[0] else 'N/A' for i in file]


        self.data_dict = {
            'format': self.format,
            'file': self.fileurl,
            'page': self.page,
            'limit': self.limit,
            'number_of_results': self.number_of_results,
            'id': self.id,
            'series_title': self.title,
            'author_name': self.author,
            'abstract': self.abstract,
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)

        


class Search:
    def __init__(self, data):
        records = data.get('records', [])

        self.file_url = [i[0].get('fileurl', '') for i in [i.get('file') for i in records] if i and i[0]]
        self.title = [i.get('title', '') for i in records]
        
        author = [item for sublist in [i.get('author', []) for i in records] for item in sublist]
        self.author_name = [i.get('name', '') for i in author]
        
        self.abstract = [i.get('abstract', '') for i in records]
        self.keywords = [i.get('keywords', []) for i in records]
        self.seriesHandle = [i.get('seriesHandle', '') for i in records]

        self.data_dict = { 
            'url': self.file_url,
            'title': self.title,
            'author_name': self.author_name,
            'abstract': self.abstract,
            'keywords': self.keywords,
            'seriesHandle': self.seriesHandle
        }

        # Pad all columns to the same length
        max_len = max(len(column) for column in self.data_dict.values())
        for key in self.data_dict:
            self.data_dict[key] += [''] * (max_len - len(self.data_dict[key]))

        self.as_dataframe = pd.DataFrame(self.data_dict).fillna('')
        #self.as_dataframe.to_csv('fudstop/data/fed_print/search_results.csv', index=False)



class Item:
    def __init__(self, records):
        self.id = [i.get('id') for i in records]

        self.seriesHandle= [i.get('seriesHandle') for i in records]
        self.doi= [i.get('doi') for i in records]
        self.jel= [i.get('jel') for i in records]
        file= [i.get('file') for i in records]
        if file is not None:
            self.fileurl = [i[0].get('fileurl') for i in file if i and i[0]]


        self.title= [i.get('title') for i in records]
        self.author= [i.get('author') for i in records]
        self.number= [i.get('number') for i in records]
        self.abstract= [i.get('abstract') for i in records]
        self.keywords= [i.get('keywords') for i in records]
        self.relatedworks= [i.get('relatedworks') for i in records]


        self.data_dict = { 

            'series_handle': self.seriesHandle,
            'doi': self.doi,
            'jel': self.jel,
            'file': self.fileurl,
            'title': self.title,
            'author': self.author,
            'number': self.number,
            'abstract': self.abstract,
            'keywords': self.keywords,
            'related_works': self.relatedworks

        }

        self.as_dataframe = pd.DataFrame(self.data_dict)

        #self.as_dataframe.to_csv('fudstop/data/fed_print/item.csv', index=False)




class SingleItem:
    def __init__(self, records):
        self.id = records.get('id')

        self.seriesHandle= records.get('seriesHandle')
        self.doi= records.get('doi')
        self.jel= records.get('jel')
        file= records.get('file')
        if file is not None:
            self.fileurl = [i.get('fileurl') for i in file]
        else:
            self.fileurl = None


        self.title= records.get('title')
        self.author= records.get('author')
        self.number= records.get('number')
        self.abstract= records.get('abstract')
        self.keywords= records.get('keywords')
        self.relatedworks= records.get('relatedworks')


        self.data_dict = { 

            'series_handle': self.seriesHandle,
            'doi': self.doi,
            'jel': self.jel,
            'file': self.fileurl if self.fileurl is not None else None,
            'title': self.title,
            'author': self.author,
            'number': self.number,
            'abstract': self.abstract,
            'keywords': self.keywords,
            'related_works': self.relatedworks

        }

        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])

        #self.as_dataframe.to_csv('fudstop/data/fed_print/item.csv', index=False)