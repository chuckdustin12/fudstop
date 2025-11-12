import pandas as pd




class IndustryPerformances:
    def __init__(self, data):

        self.id = [i.get('id') for i in data]
        self.name = [i.get('name') for i in data]
        self.changeRatio = [round(float(i.get('changeRatio'))*100,2) for i in data]
        self.marketValue = [float(i.get('marketValue')) for i in data]
        self.volume = [float(i.get('volume')) for i in data]
        self.declinedNum = [float(i.get('declinedNum')) for i in data]
        self.advancedNum = [float(i.get('advancedNum')) for i in data]
        self.flatNum = [float(i.get('flatNum')) for i in data]



        self.data_dict = { 
            'id': self.id,
            'name': self.name,
            'change_pct': self.changeRatio,
            'market_value': self.marketValue,
            'volume': self.volume,
            'declined': self.declinedNum,
            'advanced': self.advancedNum,
            'flat': self.flatNum,
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)