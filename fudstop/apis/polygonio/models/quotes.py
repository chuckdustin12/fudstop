import pandas as pd



class StockQuotes:
    def __init__(self, results):

        self.ask_exchange = [i.get('ask_exchange') for i in results]
        self.ask_price = [i.get('ask_price') for i in results]
        self.ask_size = [i.get('ask_size') for i in results]
        self.bid_exchange = [i.get('bid_exchange') for i in results]
        self.bid_price = [i.get('bid_price') for i in results]
        self.bid_size = [i.get('bid_size') for i in results]
        self.indicators = [','.join(i.get('indicators')) for i in results]
        self.participant_timestamp = [i.get('participant_timestamp') for i in results]
        self.sequence_number = [i.get('sequence_number') for i in results]
        self.sip_timestamp = [i.get('sip_timestamp') for i in results]
        self.tape = [i.get('tape') for i in results]


        self.data_dict = { 
            'ask_exchange': self.ask_exchange,
            'bid_exchange': self.bid_exchange,
            'ask_size': self.ask_size,
            'ask_price': self.ask_price,
            'bid_size': self.bid_size,
            'bid_price': self.bid_price,
            'timestamp': self.sip_timestamp,
            'tape': self.tape
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class LastStockQuote:
    def __init__(self, results):

        self.ask_price = results.get('P', 0)
        self.ask_size = results.get('S', 0)
        self.ticker = results.get('T')
        self.ask_exchange = results.get('X')
        self.indicators = results.get('i', 0)
        self.bid_price = results.get('p')
        self.bid_size = results.get('s')
        self.timestamp = results.get('t')
        self.bid_exchange = results.get('x')
        self.tape = results.get('z')
        self.conditions = results.get('c', 0)


        self.data_dict = { 
            'ask': self.ask_price,
            'ask_size': self.ask_size,
            'ask_exchange': self.ask_exchange,
            'bid': self.bid_price,
            'bid_size': self.bid_size,
            'bid_exchange': self.bid_exchange,
            'indicators': self.indicators,
            'conditions': self.conditions,
            'timestamp': self.timestamp,
            'tape': self.tape



        }


        self.as_dataframe = pd.DataFrame(self.data_dict)
        