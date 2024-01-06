import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))
from webull_helpers import format_date
import pandas as pd


class Event:
    def __init__(self, data):
        self.actual = data.get('actual', None)
        self.comment = data.get('comment', None)
        self.country = data['country']
        self.currency = data['currency']
        self.date = data['date']
        self.time = format_date(self.date)
        self.forecast = data['forecast']
        self.id = data['id']
        self.importance = data['importance']
        self.indicator = data['indicator']
        self.link = data['link']
        self.period = data['period']
        self.previous = data['previous']
        self.scale = data['scale']
        self.source = data['source']
        self.title = data['title']
        self.unit = data['unit']


        self.data_dict = { 
        'actual': self.actual,
        'comment': self.comment,
        'country': self.country,
        'currency': self.currency,
        'date': self.date,
        'time': self.time,
        'forecast': self.forecast,
        'id': self.id,
        'importance': self.importance,
        'indicator': self.indicator,
        'link': self.link,
        'period': self.period,
        'previous': self.previous,
        'scale': self.scale,
        'source': self.source,
        'title': self.title,
        'unit': self.unit
    }
        self.df = pd.DataFrame(self.data_dict)
    def __str__(self):
        return f"Event(id={self.id}, date={self.date}, time={self.time}, title={self.title}, importance={self.importance})"

    def __repr__(self):
        return self.__str__()