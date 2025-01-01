import pandas as pd

class Analysis:
    def __init__(self, r=None):
        if r is not None:
            self.rating = r.get('rating', None)
            if self.rating:
                self.rating_suggestion = self.rating.get('ratingAnalysis', None)

                if 'ratingAnalysisTotals' in self.rating:
                    self.rating_totals = self.rating['ratingAnalysisTotals']
                else:
                    self.rating_totals = None

                rating_spread = self.rating.get('ratingSpread', None)
                if rating_spread:
                    self.buy = float(rating_spread.get('buy')) if rating_spread.get('buy') is not None else None
                    self.underperform = float(rating_spread.get('underPerform')) if rating_spread.get('underPerform') is not None else None
                    self.strongbuy = float(rating_spread.get('strongBuy')) if rating_spread.get('strongBuy') is not None else None
                    self.sell = float(rating_spread.get('sell')) if rating_spread.get('sell') is not None else None
                    self.hold = float(rating_spread.get('hold')) if rating_spread.get('hold') is not None else None

                    self.data_dict = { 
                        'strong_buy': self.strongbuy,
                        'buy': self.buy,
                        'hold': self.hold,
                        'underperform': self.underperform,
                        'sell': self.sell,
                    }

                    self.df = pd.DataFrame(self.data_dict, index=[0])
                
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
