import pandas as pd




class EarningsCalendar:
    def __init__(self, data):


        self.symbol = [i.get('symbol') for i in data]
        self.sk = [i.get('sk') for i in data]
        self.earningsDate = [i.get('earningsDate') for i in data]
        self.earningsTime = [i.get('earningsTime') for i in data]
        self.isDateConfirmed = [i.get('isDateConfirmed') for i in data]
        self.marketCap = [i.get('marketCap') for i in data]
        self.date = [i.get('date') for i in data]



        self.data_dict = { 
            'ticker': self.symbol,
            'sk': self.sk,
            'er_date': self.earningsDate,
            'er_time': self.earningsTime,
            'confirmed': self.isDateConfirmed,
            'market_cap': self.marketCap,
            'date': self.date
        }



        self.df = pd.DataFrame(self.data_dict)



class News:
    def __init__(self, data):

        self.id = [i.get('id') for i in data]
        self.createdDate = [i.get('createdDate') for i in data]
        self.title = [i.get('title') for i in data]
        self.isHeadline = [i.get('isHeadline') for i in data]
        self.teaser = [i.get('teaser') for i in data]
        self.symbols = [i.get('symbols') for i in data]
        self.image = [i.get('image') for i in data]
        self.slug = [i.get('slug') for i in data]
        self.newsType = [i.get('newsType') for i in data]
        self.externalId = [i.get('externalId') for i in data]

        self.data_dict = { 
            'id': self.id,
            'date': self.createdDate,
            'title': self.title,
            'is_headline': self.isHeadline,
            'teaser': self.teaser,
            'tickers': ','.join(self.symbols),
            'image': self.image,
            'slug': self.slug,
            'type': self.newsType,
            'external_id': self.externalId
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)