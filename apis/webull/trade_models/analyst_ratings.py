import pandas as pd

class Analysis:
    def __init__(self, r):
        
        self.rating = r.get('rating', None)
        if self.rating:
            self.rating_suggestion = self.rating.get('ratingAnalysis', None)

            if 'ratingAnalysisTotals' in self.rating:
                self.rating_totals = self.rating['ratingAnalysisTotals']
            else:
                self.rating_totals = None

            rating_spread = self.rating.get('ratingSpread', None)
            if rating_spread:
                self.buy = rating_spread.get('buy', None)
                self.underperform = rating_spread.get('underPerform', None)
                self.strongbuy = rating_spread.get('strongBuy', None)
                self.sell = rating_spread.get('sell', None)
                self.hold = rating_spread.get('hold', None)

                self.data_dict = { 
                    'strong_buy': self.strongbuy,
                    'buy': self.buy,
                    'hold': self.hold,
                    'underperform': self.underperform,
                    'sell': self.sell,
                }

                self.df = pd.DataFrame(self.data_dict, index=[0]).transpose()
            
            else:
                self.buy = None
                self.underperform = None
                self.strongbuy = None
                self.sell = None
                self.hold = None
                self.data_dict = None
                self.df = None
        else:
            self.rating_suggestion = None
            self.rating_totals = None
            self.buy = None
            self.underperform = None
            self.strongbuy = None
            self.sell = None
            self.hold = None
            self.df = None
            self.data_dict = None
