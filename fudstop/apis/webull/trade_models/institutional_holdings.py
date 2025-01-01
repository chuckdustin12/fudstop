import pandas as pd

class InstitutionHolding:
    def __init__(self, data):
        self.institution_holding = InstitutionStat(data['institutionHolding'])



class InstitutionStat:
    def __init__(self, data):
        self.stat = Stat(data['institutionHolding']['stat'])
        self.new_position = Position(data['institutionHolding']['newPosition'])
        self.increase = Position(data['institutionHolding']['increase'])
        self.sold_out = Position(data['institutionHolding']['soldOut'])
        self.decrease = Position(data['institutionHolding']['decrease'])

        self.data_dict = { 
            'holding_ratio': float(self.stat.holding_ratio) if self.stat.holding_ratio is not None else None,
            'holding_count_change': float(self.stat.holding_count_change) if self.stat.holding_count_change is not None else None,
            'holding_ratio_change': float(self.stat.holding_ratio_change) if self.stat.holding_ratio_change is not None else None,
            'holding_count': float(self.stat.holding_count) if self.stat.holding_count is not None else None,
            'holding_ratio': float(self.stat.holding_ratio) if self.stat.holding_ratio is not None else None,
            'new_holding_change': float(self.new_position.holding_count_change) if self.new_position.holding_count_change is not None else None,
            'new_institutional_count': float(self.new_position.institutional_count) if self.new_position.institutional_count is not None else None,
            'increase_institutional_count': float(self.increase.institutional_count) if self.increase.institutional_count is not None else None,
            'increase_holding_change': float(self.increase.holding_count_change) if self.increase.holding_count_change is not None else None,
            'sold_out_holding_change': float(self.sold_out.holding_count_change) if self.sold_out.holding_count_change is not None else None,
            'sold_out_institutional_count': float(self.sold_out.institutional_count) if self.sold_out.institutional_count is not None else None,
            'decrease_institutional_count': float(self.decrease.institutional_count) if self.decrease.institutional_count is not None else None,
            'decrease_holding_change': float(self.decrease.holding_count_change) if self.decrease.holding_count_change is not None else None,
        }



        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])
class Stat:
    def __init__(self, data):
        self.holding_count = data.get('holdingCount', None)
        self.holding_count_change = data.get('holdingCountChange', None)
        self.holding_ratio = data.get('holdingRatio', None)
        self.holding_ratio_change = data.get('holdingRatioChange', None)
        self.institutional_count = data.get('institutionalCount', None)

    def to_dict(self):
        return {
            'holdingCount': self.holding_count,
            'holdingCountChange': self.holding_count_change,
            'holdingRatio': self.holding_ratio,
            'holdingRatioChange': self.holding_ratio_change,
            'institutionalCount': self.institutional_count,
        }

class Position:
    def __init__(self, data):
        self.holding_count_change = data.get('holdingCountChange', None)
        self.institutional_count = data.get('institutionalCount', None)

    def to_dict(self):
        return {
            'holdingCountChange': self.holding_count_change,
            'institutionalCount': self.institutional_count,
        }
