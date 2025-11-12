import pandas as pd




class AINews:
    def __init__(self, data):


        self.id = [i.get('id') for i in data]
        self.title = [i.get('title') for i in data]
        self.newsUrl = [i.get('newsUrl') for i in data]
        self.siteType = [i.get('siteType') for i in data]
        self.newsTime = [i.get('newsTime') for i in data]
        self.sourceName = [i.get('sourceName') for i in data]
        self.likes = [i.get('likes') for i in data]
        self.views = [i.get('views') for i in data]
        self.accountImage = [i.get('accountImage') for i in data]
        self.relTickers = [i.get('relTickers') for i in data]
        self.accountId = [i.get('accountId') for i in data]
        self.translated = [i.get('translated') for i in data]
        self.summary = [i.get('summary') for i in data]
        self.sentiment = [i.get('sentiment') for i in data]
        self.sentimentType = [i.get('sentimentType') for i in data]

        self.data_dict = { 
            'id': self.id,
            'title': self.title,
            'url': self.newsUrl,
            'site': self.siteType,
            'time': self.newsTime,
            'source': self.sourceName,
            'likes': self.likes,
            'views': self.views,
            'related_tickers': self.relTickers,
            'summary': self.summary,
            'sentiment': self.sentiment,
            'sentiment_type': self.sentimentType

        }


        self.as_dataframe = pd.DataFrame(self.data_dict)
        