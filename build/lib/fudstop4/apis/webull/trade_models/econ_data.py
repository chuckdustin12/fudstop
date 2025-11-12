import pandas as pd



class EconomicData:
    def __init__(self, data):

        self.defId = [i.get('defId') for i in data]
        self.srcId = [i.get('srcId') for i in data]
        self.publishDateTime = [i.get('publishDateTime') for i in data]
        self.publishDate = [i.get('publishDate') for i in data]
        self.unit = [i.get('unit') for i in data]
        self.priorValue = [i.get('priorValue') for i in data]
        self.period = [i.get('period') for i in data]
        self.source = [i.get('source') for i in data]
        self.type = [i.get('type') for i in data]
        self.name = [i.get('name') for i in data]
        self.indicatorType = [i.get('indicatorType') for i in data]
        self.frequency = [i.get('frequency') for i in data]
        self.regionCode = [i.get('regionCode') for i in data]
        self.frequencyName = [i.get('frequencyName') for i in data]


        self.data_dict = { 
            'def_id': self.defId,
            'src_id': self.srcId,
            'publish_date': self.publishDate,
            'publish_datetime': self.publishDateTime,
            'unit': self.unit,
            'prior_val': self.priorValue,
            'period': self.period,
            'source': self.source,
            'type': self.type,
            'name': self.name,
            'indicator': self.indicatorType,
            'frequency': self.frequency,
            'frequency_name': self.frequencyName
              }
        

        self.as_dataframe = pd.DataFrame(self.data_dict)



class EconEvents:
    def __init__(self, data):
        self.src_id = [i.get('srcId') for i in data]
        self.publish_datetime = [i.get('publishDateTime') for i in data]
        self.publish_date = [i.get('publishDate') for i in data]
        self.source = [i.get('source') for i in data]
        self.name = [i.get('eventName') for i in data]

        self.data_dict = { 
            'src_id': self.src_id,
            'publish_datetime': self.publish_datetime,
            'publish_date': self.publish_date,
            'source': self.source,
            'event_name': self.name
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)