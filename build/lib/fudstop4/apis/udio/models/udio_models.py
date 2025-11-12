import pandas as pd




class MySongs:
    def __init__(self, data):


        self.id = [i.get('id') for i in data]
        self.user_id = [i.get('user_id') for i in data]
        self.artist = [i.get('artist') for i in data]
        self.title = [i.get('title') for i in data]
        self.created_at = [i.get('created_at') for i in data]
        self.error_id = [i.get('error_id') for i in data]
        self.error_type = [i.get('error_type') for i in data]
        self.generation_id = [i.get('generation_id') for i in data]
        self.image_path = [i.get('image_path') for i in data]
        self.lyrics = [i.get('lyrics') for i in data]
        self.prompt = [i.get('prompt') for i in data]
        self.likes = [i.get('likes') for i in data]
        self.plays = [i.get('plays') for i in data]
        self.published_at = [i.get('published_at') for i in data]
        self.song_path = [i.get('song_path') for i in data]
        # self.tags = [i.get('tags') for i in data]
        # self.tags = [item for sublist in self.tags for item in sublist if isinstance(sublist, list)]
        self.duration = [i.get('duration') for i in data]
        self.video_path = [i.get('video_path') for i in data]
        self.error_detail = [i.get('error_detail') for i in data]



        self.as_dataframe = pd.DataFrame(self.data_dict())
    def parse_tags(self, tags_data):
        if isinstance(tags_data, list):
            # If tags_data is already a list, return it directly
            return tags_data
        elif isinstance(tags_data, str):
            # Split the tags_string by comma and strip whitespace from each tag
            return [tag.strip() for tag in tags_data.split(',')]
        else:
            return []

    def data_dict(self):
        result = {}
        for key in vars(self):
            if key == 'data':
                continue
            if key == 'tags':
                # Parse tags further if the key is 'tags'
                result[key] = [self.parse_tags(tag) for tag in getattr(self, key)]
            else:
                if isinstance(getattr(self, key), list):
                    result[key] = getattr(self, key)
                else:
                    # Handle nested dictionaries
                    result[key] = [i.get(key) for i in self.data]
        return result