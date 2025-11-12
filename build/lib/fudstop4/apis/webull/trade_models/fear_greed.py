import pandas as pd





class FearGreedCurrent:
    def __init__(self, current):
        self.date = current.get('date')
        self.feargreed = current.get('fearGreedIndex')
        self.grade = current.get('grade')
        self.describe = current.get('describe')

        self.data_dict = { 
            'date': self.date,
            'fear_greed': self.feargreed,
            'grade': self.grade,
            'describe': self.describe
        }



        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])
