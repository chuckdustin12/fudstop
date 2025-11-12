import pandas as pd



class Screener:
    def __init__(self, rows):

        self.id = [i.get('id') for i in rows]
        self.exchangeSymbol = [i.get('exchangeSymbol') for i in rows]
        self.ticker = [i.get('ticker') for i in rows]
        self.generalMktCapUsd = [i.get('generalMktCapUsd') for i in rows]
        self.subIndustry = [i.get('subIndustry') for i in rows]
        self.priceChange3m = [i.get('priceChange3m') for i in rows]
        self.shortScore = [i.get('shortScore') for i in rows]
        self.ffOnLoan = [i.get('ffOnLoan') for i in rows]
        self.ffEst = [i.get('ffEst') for i in rows]
        self.util = [i.get('util') for i in rows]
        self.dtc = [i.get('dtc') for i in rows]
        self.c2b = [i.get('c2b') for i in rows]
        self.shortOnloanPercentile1y = [i.get('shortOnloanPercentile1y') for i in rows]
        self.epsEpsmom1m = [i.get('epsEpsmom1m') for i in rows]
        self.analystCoverage = [i.get('analystCoverage') for i in rows]
        self.analystTp = [i.get('analystTp') for i in rows]



        # Create the self.data_dict
        self.data_dict = {
            'id': self.id,
            'exchange_symbol': self.exchangeSymbol,
            'ticker': self.ticker,
            'general_mkt_cap_usd': self.generalMktCapUsd,
            'sub_industry': self.subIndustry,
            'price_change_3m': self.priceChange3m,
            'short_score': self.shortScore,
            'ff_on_loan': self.ffOnLoan,
            'ff_est': self.ffEst,
            'util': self.util,
            'dtc': self.dtc,
            'c2b': self.c2b,
            'short_onloan_percentile_1y': self.shortOnloanPercentile1y,
            'eps_epsmom_1m': self.epsEpsmom1m,
            'analyst_coverage': self.analystCoverage,
            'analyst_tp': self.analystTp
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)