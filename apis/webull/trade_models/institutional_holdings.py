class InstitutionHolding:
    def __init__(self, data):
        self.institution_holding = InstitutionStat(data['institutionHolding'])

    def to_dict(self):
        return {
            'institutionHolding': self.institution_holding.to_dict()
        }

class InstitutionStat:
    def __init__(self, data):
        self.stat = Stat(data['institutionHolding']['stat'])
        self.new_position = Position(data['institutionHolding']['newPosition'])
        self.increase = Position(data['institutionHolding']['increase'])
        self.sold_out = Position(data['institutionHolding']['soldOut'])
        self.decrease = Position(data['institutionHolding']['decrease'])

    def to_dict(self):
        return {
            'stat': self.stat.to_dict(),
            'newPosition': self.new_position.to_dict(),
            'increase': self.increase.to_dict(),
            'soldOut': self.sold_out.to_dict(),
            'decrease': self.decrease.to_dict(),
        }

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
