import pandas as pd

import requests

class DataGOVSDK:
    def __init__(self):
        pass


    def get_mega_millions(self):
        r = requests.get("https://data.ny.gov/api/views/5xaw-6ayf/rows.json?accessType=DOWNLOAD").json()

        data = r['data']

        # Extract relevant data
        extracted_data = [
            {
                "Date": row[8].split('T')[0],
                "Winning Numbers": row[9],
                "Mega Ball": row[10]
            }
            for row in data
        ]


        df = pd.DataFrame(extracted_data)

        return df
    


    def electric_vehicle_population(self):
        r = requests.get("https://data.wa.gov/api/views/f6w7-q2d2/rows.json?accessType=DOWNLOAD").json()


        data = r['data']

        extracted_data = [
            {
                "VIN": row[8],
                "County": row[9],
                "City": row[10],
                "State": row[11],
                "ZIP Code": row[12],
                "Model Year": row[13],
                "Make": row[14],
                "Model": row[15],
                "Fuel Type": row[16],
                "Eligibility": row[17],
                "CO2 Emission": row[18],
                "MPG Equivalent": row[19],
                "Electric Range (miles)": row[20],
                "Coordinates": row[22],
                "Utility Provider": row[23],
            }
            for row in data
        ]


        df = pd.DataFrame(extracted_data)

        return df
    

    def obesity_by_state(self):
        r = requests.get("https://data-lakecountyil.opendata.arcgis.com/api/download/v1/items/3e0c1eb04e5c48b3be9040b0589d3ccf/geojson?layers=8").json()


        features = r['features']

        properties = [i.get('properties') for i in features]

        name = [i.get('NAME') for i in properties]
        obs = [i.get('Obesity') for i in properties]

        dict = { 
            'name': name,
            'obesity': obs
        }


        df = pd.DataFrame(dict).sort_values('obesity', ascending=False)

        return df
    

    def border_control(self):
        r = requests.get("https://data.bts.gov/views/keg4-3bc2/rows.json?accessType=DOWNLOAD").json()

        data = r['data']

        # Extract relevant data
        extracted_data = [
            {
                "Location": row[8],
                "State": row[9],
                "Port Code": row[10],
                "Border": row[11],
                "Date": row[12].split('T')[0],
                "Vehicle Type": row[13],
                "Volume": row[14],
                "Latitude": row[15],
                "Longitude": row[16],
                "Coordinates": row[17]
            }
            for row in data
        ]


        df = pd.DataFrame(extracted_data)


        return df
    


    def crime(self):
        r = requests.get("https://opendata.dc.gov/api/download/v1/items/c5a9f33ffca546babbd91de1969e742d/geojson?layers=6").json()


        features = r['features']

        properties = [i.get('properties') for i in features]

        CCN = [i.get('CCN') for i in properties]
        REPORT_DAT = [i.get('REPORT_DAT') for i in properties]
        SHIFT = [i.get('SHIFT') for i in properties]
        METHOD = [i.get('METHOD') for i in properties]
        OFFENSE = [i.get('OFFENSE') for i in properties]
        BLOCK = [i.get('BLOCK') for i in properties]
        XBLOCK = [i.get('XBLOCK') for i in properties]
        YBLOCK = [i.get('YBLOCK') for i in properties]
        WARD = [i.get('WARD') for i in properties]
        ANC = [i.get('ANC') for i in properties]
        DISTRICT = [i.get('DISTRICT') for i in properties]
        PSA = [i.get('PSA') for i in properties]
        NEIGHBORHOOD_CLUSTER = [i.get('NEIGHBORHOOD_CLUSTER') for i in properties]
        BLOCK_GROUP = [i.get('BLOCK_GROUP') for i in properties]
        CENSUS_TRACT = [i.get('CENSUS_TRACT') for i in properties]
        VOTING_PRECINCT = [i.get('VOTING_PRECINCT') for i in properties]
        LATITUDE = [i.get('LATITUDE') for i in properties]
        LONGITUDE = [i.get('LONGITUDE') for i in properties]
        BID = [i.get('BID') for i in properties]
        START_DATE = [i.get('START_DATE') for i in properties]
        END_DATE = [i.get('END_DATE') for i in properties]
        OBJECTID = [i.get('OBJECTID') for i in properties]
        OCTO_RECORD_ID = [i.get('OCTO_RECORD_ID') for i in properties]

        dict = { 
            'ccn': CCN,
            'report_date': REPORT_DAT,
            'shift': SHIFT,
            'method': METHOD,
            'offense': OFFENSE,
            'block': BLOCK,
            'xblock': XBLOCK,
            'yblock': YBLOCK,
            'ward': WARD,
            'anc': ANC,
            'district':  DISTRICT,
            'psa': PSA,
            'neighborhood_cluster': NEIGHBORHOOD_CLUSTER,
            'block_group': BLOCK_GROUP,
            'census_tract': CENSUS_TRACT,
            'voting_precinct': VOTING_PRECINCT,
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'bid': BID,
            'start_date': START_DATE,
            'end_date': END_DATE,
            'objectid': OBJECTID,
            'octo_record_id': OCTO_RECORD_ID

        }


        df = pd.DataFrame(dict)

        return df