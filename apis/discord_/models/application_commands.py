import pandas as pd



class Applications:
    def __init__(self, applications):
        self.id = [i.get('id') for i in applications]
        self.name = [i.get('name') for i in applications]
        self.description = [i.get('description') for i in applications]
        self.icon = [i.get('icon') for i in applications]
        
        
        self.bots = [i.get('bot') for i in applications]

        self.id = [i.get('id') for i in self.bots]
        self.username = [i.get('username') for i in self.bots]
        self.discriminator = [i.get('discriminator') for i in self.bots]
        self.bot = [i.get('bot') for i in self.bots]
        self.public_flags = [i.get('public_flags') for i in self.bots]
        self.flags = [i.get('flags') for i in self.bots]
        self.global_name = [i.get('global_name') for i in self.bots]
        self.avatar = [i.get('avatar') for i in self.bots]
        self.accent_color = [i.get('accent_color') for i in self.bots]
        self.avatar_decoration_data = [i.get('avatar_decoration_data') for i in self.bots]
        self.banner = [i.get('banner') for i in self.bots]
        self.banner_color = [i.get('banner_color') for i in self.bots]


        self.data_dict = { 
            'id': self.id,
            'username': self.username,
            'discriminator': self.discriminator,
            'bot': self.bot,
            'public_flags': self.public_flags,
            'flags': self.flags,
            'global_name': self.global_name,
            'avatar': self.avatar,
            'accent_color': self.accent_color,
            'avatar_decoration_data': self.avatar_decoration_data,
            'banner': self.banner,
            'banner_color': self.banner_color
        }


        self.applications_as_dataframe = pd.DataFrame(self.data_dict)


class ApplicationCommands:
    def __init__(self, application_commands):

        self.id = [i.get('id') for i in application_commands]
        self.type = [i.get('type') for i in application_commands]
        self.application_id = [i.get('application_id') for i in application_commands]
        self.version = [i.get('version') for i in application_commands]
        self.name = [i.get('name') for i in application_commands]
        self.description = [i.get('description') for i in application_commands]
        self.options = [i.get('options') for i in application_commands]
        self.options = [item for sublist in (self.options if self.options is not None else []) if sublist is not None for item in sublist]
        #self.options_name = [i.get('name') for i in self.options]
        #self.options_type = [i.get('type') for i in self.options]
        #self.options_desc = [i.get('description') for i in self.options]
        #self.options_required = [i.get('required') for i in self.options]
        self.options_options = [i.get('options') for i in self.options if i is not None and i.get('options') is not None]
        self.options_options = [item for sublist in self.options_options for item in sublist]
        self.options_type = [i.get('type') for i in self.options_options]
        self.options_name = [i.get('name') for i in self.options_options]
        self.options_description = [i.get('description') for i in self.options_options]
        self.options_required = [i.get('required') for i in self.options_options]
        self.autocomplete = [i.get('autocomplete') for i in self.options_options]

        
        self.integration_types = [i.get('integration_types') for i in application_commands]
        # Pad all other attributes to match the length of self.options\
        max_length = len(self.options)
        self.id = self.pad_list(self.id, max_length)
        self.type = self.pad_list(self.type, max_length)
        self.application_id = self.pad_list(self.application_id, max_length)
        self.version = self.pad_list(self.version, max_length)
        self.name = self.pad_list(self.name, max_length)
        self.description = self.pad_list(self.description, max_length)
        self.integration_types = self.pad_list(self.integration_types, max_length)
        self.autocomplete = self.pad_list(self.autocomplete, max_length)
        self.options_required = self.pad_list(self.options_required, max_length)
        self.options_description = self.pad_list(self.options_description, max_length)
        self.options_name = self.pad_list(self.options_name, max_length)
        self.options_type = self.pad_list(self.options_type, max_length)
        print(len(self.id))
        print(len(self.type))
        print(len(self.application_id))
        print(len(self.version), len(self.name), len(self.description), len(self.options_name))

# Assuming self.options is the longest list
        
        self.data_dict = { 
            'id': self.id,
            'type': self.type,
            'application_id': self.application_id,
            'version': self.version,
            'name': self.name,
            'description': self.description,
            'integration_types': self.integration_types,
            'option_name': self.options_name,
            'option_desc': self.options_description,
            'autocomplete':  self.autocomplete,
            'required': self.options_required
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



    # Function to pad lists to the same length
    def pad_list(self, lst, length):
        return lst + [None] * (length - len(lst))
