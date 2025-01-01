import pandas as pd


class CompanyBrief:
    def __init__(self, data):

        self.name = data.get('name')
        self.address = data.get('address')
        self.phone = data.get('phone')
        self.fax = data.get('fax')
        self.introduce = data.get('introduce')
        self.industry = data.get('industry')
        self.listDate = data.get('listDate')
        self.establishDate = data.get('establishDate')
        self.webSite = data.get('webSite')


        self.data_dict = { 
            'name': self.name,
            'address': self.address,
            'phone': self.phone,
            'fax': self.fax,
            'info': self.introduce,
            'industry': self.industry,
            'list_date': self.listDate,
            'establish_date': self.establishDate,
            'website': self.webSite

        }


        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])


class Sectors:
    def __init__(self, data):

        self.id = [i.get('id') for i in data]
        self.regionId = [i.get('regionId') for i in data]
        self.name = [i.get('name') for i in data]
        self.changeRatio = [i.get('changeRatio') for i in data]
        self.declinedNum = [i.get('declinedNum') for i in data]
        self.advancedNum = [i.get('advancedNum') for i in data]
        self.flatNum = [i.get('flatNum') for i in data]


        self.data_dict = { 
            'id': self.id,
            'region_id': self.regionId,
            'sector_name': self.name,
            'change_ratio': self.changeRatio,
            'decline_number': self.declinedNum,
            'advance_number': self.advancedNum,
            'flat_number': self.flatNum
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)


class Executives:
    def __init__(self, data):
        self.position = [i.get('position') for i in data]
        self.name = [i.get('name') for i in data]


        self.data_dict = { 
            'exec_name': self.name,
            'exec_position': self.position
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)