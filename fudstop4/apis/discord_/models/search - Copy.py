import pandas as pd


class Messages:
    def __init__(self, data):
        """Uses the Discord API to programatically search message history
        
        this is the data container for Search
        """


        # self.analytics_id = data['analytics_id']
        # self.doing_deep_historical_index = data['doing_deep_historical_index']
        # self.total_results = data['total_results']
        messages = data['messages']

        self.messages = [item for sublist in messages for item in sublist]

        self.id = [i.get('id') for i in self.messages]
        self.type = [i.get('type') for i in self.messages]
        self.content = [i.get('content') for i in self.messages]
        self.channel_id = [i.get('channel_id') for i in self.messages]
        self.author = [i.get('author') for i in self.messages]
        self.attachments = [i.get('attachments') for i in self.messages]
        self.embeds = [i.get('embeds') for i in self.messages]
        self.mentions = [i.get('mentions') for i in self.messages]
        self.mention_roles = [i.get('mention_roles') for i in self.messages]
        self.pinned = [i.get('pinned') for i in self.messages]
        self.mention_everyone = [i.get('mention_everyone') for i in self.messages]
        self.tts = [i.get('tts') for i in self.messages]
        self.timestamp = [i.get('timestamp') for i in self.messages]
        self.edited_timestamp = [i.get('edited_timestamp') for i in self.messages]
        self.flags = [i.get('flags') for i in self.messages]
        self.components = [i.get('components') for i in self.messages]
        self.hit = [i.get('hit') for i in self.messages]



class Attachments:
    def __init__(self, attachments):

        self.id = [i.get('id') for i in attachments]
        self.filename = [i.get('filename') for i in attachments]
        self.size = [i.get('size') for i in attachments]
        self.url = [i.get('url') for i in attachments]
        self.proxy_url = [i.get('proxy_url') for i in attachments]
        self.duration_secs = [i.get('duration_secs') for i in attachments]
        self.waveform = [i.get('waveform') for i in attachments]
        self.content_type = [i.get('content_type') for i in attachments]
        self.content_scan_version = [i.get('content_scan_version') for i in attachments]


        self.data_dict = { 
            'id': self.id,
            'filename': self.filename,
            'size': self.size,
            'url': self.url,
            'proxy_url': self.proxy_url,
            'duration_secs': self.duration_secs,
            'waveform': self.waveform,
            'content_type': self.content_type,
            'content_scan_version': self.content_scan_version
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)