import pandas as pd
class Webhooks:
    def __init__(self, webhooks):


        self.application_id = [i.get('application_id') for i in webhooks]
        self.avatar = [i.get('avatar') for i in webhooks]
        self.channel_id = [i.get('channel_id') for i in webhooks]
        self.guild_id = [i.get('guild_id') for i in webhooks]
        self.id = [i.get('id') for i in webhooks]
        self.name = [i.get('name') for i in webhooks]
        self.type = [i.get('type') for i in webhooks]
        self.token = [i.get('token') for i in webhooks]
        self.url = [i.get('url') for i in webhooks]
        self.user = [i.get('user') for i in webhooks]




        self.data_dict = { 
            'application_id': self.application_id,
            'avatar': self.avatar,
            'channel_id': self.channel_id,
            'guild_id': self.guild_id,
            'id': self.id,
            'name': self.name,
            'type': self.type,
            'token': self.token,
            'url': self.url,
            'user': self.user
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)