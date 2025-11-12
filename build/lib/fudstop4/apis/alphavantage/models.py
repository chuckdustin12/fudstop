import pandas as pd


class HistoricOptions:
    def __init__(self, data):

        self.contractID = [i.get('contractID') for i in data]
        self.symbol = [i.get('symbol') for i in data]
        self.expiration = [i.get('expiration') for i in data]
        self.strike = [i.get('strike') for i in data]
        self.type = [i.get('type') for i in data]
        self.last = [i.get('last') for i in data]
        self.mark = [i.get('mark') for i in data]
        self.bid = [i.get('bid') for i in data]
        self.bid_size = [i.get('bid_size') for i in data]
        self.ask = [i.get('ask') for i in data]
        self.ask_size = [i.get('ask_size') for i in data]
        self.volume = [i.get('volume') for i in data]
        self.open_interest = [i.get('open_interest') for i in data]
        self.date = [i.get('date') for i in data]
        self.implied_volatility = [i.get('implied_volatility') for i in data]
        self.delta = [i.get('delta') for i in data]
        self.gamma = [i.get('gamma') for i in data]
        self.theta = [i.get('theta') for i in data]
        self.vega = [i.get('vega') for i in data]
        self.rho = [i.get('rho') for i in data]


        self.data_dict = { 
            'id': self.contractID,
            'option_symbol': self.symbol,
            'expiry': self.expiration,
            'strike': self.strike,
            'call_put': self.type,
            'last_price': self.last,
            'mark': self.mark,
            'bid': self.bid,
            'bid_size': self.bid_size,
            'ask': self.ask,
            'ask_size': self.ask_size,
            'volume': self.volume,
            'oi': self.open_interest,
            'date': self.date,
            'iv': self.implied_volatility,
            'delta': self.delta,
            'gamma': self.gamma,
            'vega': self.vega,
            'theta': self.theta,
            'rho': self.rho,
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)


class NewsSentiments:
    def __init__(self, feed):

        self.title = [i.get('title') for i in feed]
        self.url = [i.get('url') for i in feed]
        self.time_published = [i.get('time_published') for i in feed]
        self.authors = [i.get('authors') for i in feed]
        self.summary = [i.get('summary') for i in feed]
        self.banner_image = [i.get('banner_image') for i in feed]
        self.source = [i.get('source') for i in feed]
        self.category_within_source = [i.get('category_within_source') for i in feed]
        self.source_domain = [i.get('source_domain') for i in feed]
        topics = [i.get('topics') for i in feed]
        topics = [item for sublist in topics for item in sublist]
        self.topic = [i.get('topic') for i in topics]
        self.relevance = [i.get('relevance_score') for i in topics]
        self.overall_sentiment_score = [i.get('overall_sentiment_score') for i in feed]
        self.overall_sentiment_label = [i.get('overall_sentiment_label') for i in feed]
        ticker_sentiment = [i.get('ticker_sentiment') for i in feed]
        ticker_sentiment = [item for sublist in ticker_sentiment for item in sublist]
        self.ticker = [i.get('ticker') for i in ticker_sentiment]
        self.relevance_score = [i.get('relevance_score') for i in ticker_sentiment]
        self.ticker_sentiment_score = [i.get('ticker_sentiment_score') for i in ticker_sentiment]
        self.ticker_sentiment_label = [i.get('ticker_sentiment_label') for i in ticker_sentiment]


        # Find the maximum length among all lists
        max_length = max(
            len(self.title),
            len(self.url),
            len(self.time_published),
            len(self.authors),
            len(self.summary),
            len(self.banner_image),
            len(self.source),
            len(self.category_within_source),
            len(self.source_domain),
            len(self.topic),
            len(self.relevance),
            len(self.overall_sentiment_score),
            len(self.overall_sentiment_label),

        )

        # Function to pad or truncate a list to a target length
        def adjust_length(lst, length, fill_value=None):
            return lst[:length] + [fill_value] * (length - len(lst))

 
        # Create the dictionary and DataFrame as before
        self.data_dict = { 

            'title': self.title,
            'url': self.url,
            'pub_time': self.time_published,
            'authors': self.authors,
            'summary': self.summary,
            'image': self.banner_image,
            'source': self.source,
            'source_category': self.category_within_source,
            'source_url': self.source_domain,
            'sentiment_score': self.overall_sentiment_score,
            'sentiment': self.overall_sentiment_label,

        }

        self.as_dataframe = pd.DataFrame(self.data_dict)



class InsiderTransactions:
    def __init__(self, data):
        self.transaction_date=  [i.get('transaction_date') for i in data]
        self.ticker=  [i.get('ticker') for i in data]
        self.executive=  [i.get('executive') for i in data]
        self.executive_title=  [i.get('executive_title') for i in data]
        self.security_type=  [i.get('security_type') for i in data]
        self.acquisition_or_disposal=  [i.get('acquisition_or_disposal') for i in data]
        self.shares=  [i.get('shares') for i in data]
        self.share_price=  [i.get('share_price') for i in data]


        self.data_dict = { 

            'trans_date': self.transaction_date,
            'ticker': self.ticker,
            'exec': self.executive,
            'exec_title': self.executive_title,
            'security': self.security_type,
            'acquire_or_dispose': self.acquisition_or_disposal,
            'shares': self.shares,
            'price_point': self.share_price


        }


        self.as_dataframe = pd.DataFrame(self.data_dict)