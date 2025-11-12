import pandas as pd




class NasdaqRSS:
    def __init__(self, rows):
        self.title = [i.get('title') for i in rows]
        self.link = [i.get('link') for i in rows]
        self.description = [i.get('description') for i in rows]
        self.pubDate = [i.get('pubDate') for i in rows]
        self.guid = [i.get('guid') for i in rows]
        self.creator = [i.get('creator') for i in rows]
        self.category = [i.get('category') for i in rows]
        self.tickers = [i.get('tickers') for i in rows]
        self.partnerlink = [i.get('partnerlink') for i in rows]


        self.data_dict = { 
            'title': self.title,
            'link': self.link,
            'description': self.description,
            'published': self.pubDate,
            'creator': self.creator,
            'category': self.category,
            'tickers': self.tickers
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)