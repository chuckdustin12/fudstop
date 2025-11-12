import pandas as pd



class HomeSearch:
    def __init__(self, data):
        self.typename = [i.get('__typename') for i in data]
        self.property_id = [i.get('property_id') for i in data]
        self.listing_id = [i.get('listing_id') for i in data]
        self.plan_id = [i.get('plan_id') for i in data]
        self.status = [i.get('status') for i in data]
        self.photo_count = [i.get('photo_count') for i in data]
        self.branding = [i.get('branding') for i in data]



        location = [i.get('location') for i in data]
        address = [i.get('address') for i in location]
        self.street_view = [i.get('street_view_url') for i in location]
        self.city = [i.get('city') for i in address]
        self.line = [i.get('line') for i in address]
        self.street_name = [i.get('street_name') for i in address]
        self.street_number = [i.get('street_number') for i in address]
        self.street_suffix = [i.get('street_suffix') for i in address]
        self.country = [i.get('country') for i in address]
        self.postal_code = [i.get('postal_code') for i in address]
        self.state_code = [i.get('state_code') for i in address]
        self.state = [i.get('state') for i in address]
        self.coordinate = [i.get('coordinate') for i in address]
        self.open_houses = [i.get('open_houses') for i in data]


        description = [i.get('description') for i in data]
        self.sub_type = [i.get('sub_type') for i in description]
        self.type = [i.get('type') for i in description]
        self.beds = [i.get('beds') for i in description]
        self.baths = [i.get('baths') for i in description]
        self.lot_sqft = [i.get('lot_sqft') for i in description]
        self.sqft = [i.get('sqft') for i in description]
        self.beds_max = [i.get('beds_max') for i in description]
        self.beds_min = [i.get('beds_min') for i in description]
        self.sqft_max = [i.get('sqft_max') for i in description]
        self.sqft_min = [i.get('sqft_min') for i in description]
        self.baths_full = [i.get('baths_full') for i in description]
        self.baths_half = [i.get('baths_half') for i in description]
        self.baths_min = [i.get('baths_min') for i in description]
        self.baths_max = [i.get('baths_max') for i in description]
        self.baths_full_calc = [i.get('baths_full_calc') for i in description]
        self.baths_partial_calc = [i.get('baths_partial_calc') for i in description]



        self.virtual_tours = [i.get('virtual_tours') for i in data]
        self.matterport = [i.get('matterport') for i in data]
        advertisers = [i.get('advertisers') for i in data]
        advertisers = [item for sublist in advertisers for item in sublist]

        self.ad_name = [i.get('name') for i in advertisers]
        self.ad_email = [i.get('email') for i in advertisers]
        self.ad_link = [i.get('href') for i in advertisers]
        self.ad_slogan = [i.get('name') for i in advertisers]
        self.ad_type = [i.get('type') for i in advertisers]
        self.flags = [i.get('flags') for i in data]




        source = [i.get('source') for i in data]
        agents = [i.get('agents') for i in source]
        agents = [item for sublist in agents if sublist is not None for item in sublist]
        self.id = [i.get('id') for i in agents]
        self.agent_id = [i.get('agent_id') for i in agents]
        self.agent_name = [i.get('agent_name') for i in agents]
        self.agent_office_id = [i.get('office_id') for i in agents]
        self.agent_office_name = [i.get('office_name') for i in agents]

        self.source_id = [i.get('id') for i in source]
        self.source_type = [i.get('type') for i in source]
        self.spec_id = [i.get('spec_id') for i in source]
        self.plan_id = [i.get('plan_id') for i in source]
        self.listing_href = [i.get('listing_href') for i in source]
        self.listing_id = [i.get('listing_id') for i in source]





        self.pet_policy = [i.get('pet_policy') for i in data]
        self.community = [i.get('community') for i in data]
        primary_photo = [i.get('primary_photo') for i in data]
        self.primary_photo = [i.get('href') if i is not None else [] for i in primary_photo]
        self.link = [i.get('href') for i in data]
        self.list_price = [i.get('list_price') for i in data]
        self.list_price_min = [i.get('list_price_min') for i in data]
        self.list_price_max = [i.get('list_price_max') for i in data]
        self.price_reduced_amount = [i.get('price_reduced_amount') for i in data]
        estimate = [i.get('estimate', None) for i in data]
        self.estimate = [i.get('estimate', None) if i is not None else [] for i in estimate]
        self.lead_attributes = [i.get('lead_attributes') if i is not None else []for i in data ]
        self.last_sold_date = [i.get('last_sold_date') if i is not None else [] for i in data]
        self.list_date = [i.get('list_date') for i in data]
        self.products = [i.get('products') for i in data]
        self.last_sold_price = [i.get('last_sold_price') for i in data]



        self.data_dict = {
            'typename': self.typename,
            'property_id': self.property_id,
            'listing_id': self.listing_id,
            'plan_id': self.plan_id,
            'status': self.status,
            'photo_count': self.photo_count,
            'branding': self.branding,
            'street_view': self.street_view,
            'city': self.city,
            'line': self.line,
            'street_name': self.street_name,
            'street_number': self.street_number,
            'street_suffix': self.street_suffix,
            'country': self.country,
            'postal_code': self.postal_code,
            'state_code': self.state_code,
            'state': self.state,
            'coordinate': self.coordinate,
            'open_houses': self.open_houses,
            'sub_type': self.sub_type,
            'type': self.type,
            'beds': self.beds,
            'baths': self.baths,
            'lot_sqft': self.lot_sqft,
            'sqft': self.sqft,
            'beds_max': self.beds_max,
            'beds_min': self.beds_min,
            'sqft_max': self.sqft_max,
            'sqft_min': self.sqft_min,
            'baths_full': self.baths_full,
            'baths_half': self.baths_half,
            'baths_min': self.baths_min,
            'baths_max': self.baths_max,
            'baths_full_calc': self.baths_full_calc,
            'baths_partial_calc': self.baths_partial_calc,
            'virtual_tours': self.virtual_tours,
            'matterport': self.matterport,
            'advertiser_name': self.ad_name,
            'advertiser_email': self.ad_email,
            'advertiser_link': self.link,
            'advertiser_slogan': self.ad_slogan,
            'agent_id': self.agent_id,
            'agent_name': self.agent_name,
            'agent_office_id': self.agent_office_id,
            'agent_office_name': self.agent_office_name,
            'id': self.id,
            'spec_id': self.spec_id,
            'listing_href': self.listing_href,
            'pet_policy': self.pet_policy,
            'community': self.community,
            'primary_photo': self.primary_photo,
            'link': self.link,
            'list_price': self.list_price,
            'list_price_min': self.list_price_min,
            'list_price_max': self.list_price_max,
            'price_reduced_amount': self.price_reduced_amount,
            'estimate': self.estimate,
            'last_sold_date': self.last_sold_date,
            'list_date': self.list_date,
            'last_sold_price': self.last_sold_price
        }


        max_length = max(len(value) for value in self.data_dict.values() if isinstance(value, list))
        print(max_length)

        for key, value in self.data_dict.items():
            if isinstance(value, list):
                current_length = len(value)
                if current_length < max_length:
                    self.data_dict[key].extend([None] * (max_length - current_length))
        self.as_dataframe = pd.DataFrame(self.data_dict)