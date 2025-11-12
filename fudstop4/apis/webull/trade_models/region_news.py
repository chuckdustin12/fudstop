import pandas as pd



class RegionNews:
    def __init__(self, data):

        self.title = [i.get('title') for i in data]
        self.newsTime = [i.get('newsTime') for i in data]
        self.sourceName = [i.get('sourceName') for i in data]
        self.url = [i.get('url') for i in data]
        self.content = [i.get('content') for i in data]
        self.readCount = [i.get('readCount') for i in data]
        self.collectsource = [i.get('collectSource') for i in data]
        self.likes = [i.get('likes') for i in data]
        self.sentiment = [i.get('sentiment') for i in data]


        self.data_dict = { 
            'title': self.title,
            'time': self.newsTime,
            'source_name': self.sourceName,
            'url': self.url,
            'content': self.content,
            'read_count': self.readCount,
            'likes': self.likes,
            'sentiment': self.sentiment,
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)