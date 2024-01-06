from dataclasses import dataclass

from typing import Optional, List

import pandas as pd


class TickerNews:
    def __init__(self, data):

        results = data['results']

        self.amp_url = [i['amp_url'] if 'amp_url' is not None and 'amp_url' in i else None for i in results]
        self.article_url = [i['article_url'] if 'article_url' is not None and 'article_url' in i else None for i in results]
        self.author = [i['author'] if 'author' is not None and 'author' in i else None for i in results]
        self.description = [i['description'] if 'description' is not None and 'description' in i else None for i in results]
        self.id = [i['id'] if 'id' is not None and 'id' in i else None for i in results]
        self.image_url = [i['image_url'] if 'image_url' is not None and 'image_url' in i else None for i in results]
        self.keywords = [i['keywords'] if 'keywords' is not None and 'keywords' in i else None for i in results]
        
        publisher = [i['publisher'] if 'publisher' is not None and 'publisher' in i else None for i in results]
        self.tickers = [i['tickers'] if 'tickers' is not None and 'tickers' in i else None for i in results]
        self.favicon_url = [i['favicon_url'] if 'favicon_url' is not None and 'favicon_url' in i else None for i in publisher]
        self.name = [i['name'] if 'name' is not None and 'name' in i else None for i in publisher]
        self.logo_url = [i['favicon_url'] if 'favicon_url' is not None and 'favicon_url' in i else None for i in publisher]
        self.homepage_url = [i['homepage_url'] if 'homepage_url' is not None and 'homepage_url' in i else None for i in publisher]
        self.title = [i['title'] if 'title' is not None and 'title' in i else None for i in results]



        self.data_dict = {
            'Title': self.title,
            'Name': self.name,
            'Author': self.author,
            'Article URL': self.article_url,
            'Homepage URL': self.homepage_url,
            'Logo URL': self.logo_url,
            'Image URL': self.image_url,
            'Tickers Mentioned': self.tickers,
            'Description': self.description,
            'Keywords': self.keywords

        }

        self.as_dataframe = pd.DataFrame(self.data_dict)
