import pandas as pd





class SecFilings:
    def __init__(self, announcements):

        self.announcementId = [i.get('announcementId') for i in announcements]
        self.title = [i.get('title') for i in announcements]
        self.publishDate = [i.get('publishDate') for i in announcements]
        self.language = [i.get('language') for i in announcements]
        self.htmlUrl = [i.get('htmlUrl') for i in announcements]
        self.typeName = [i.get('typeName') for i in announcements]
        self.formType = [i.get('formType') for i in announcements]


        self.data_dict = { 
            'id': self.announcementId,
            'title': self.title,
            'publish_date': self.publishDate,
            'language': self.language,
            'url': self.htmlUrl,
            'type': self.typeName,
            'form': self.formType
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)