import pandas as pd



class Insiders:
    def __init__(self, data):

        try:

            self.name = [i.get('name') for i in data]
            self.transaction_date = [i.get('transactionDate') for i in data]
            self.shares = [float(i.get('shares')) for i in data]
            self.change = [i.get('change') for i in data]
            self.price = [i.get('price') for i in data]
            self.acquired = [i.get('isAcquire') for i in data]


            self.data_dict = { 
                'name': self.name,
                'transaction_date': self.transaction_date,
                'shares': self.shares,
                'change': self.change,
                'price': self.price,
                'acquired': self.acquired
            }


            self.df = pd.DataFrame(self.data_dict)
        except Exception as e:

            print(data)


        