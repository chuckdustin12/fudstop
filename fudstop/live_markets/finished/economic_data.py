import requests
import pandas as pd

def get_survey_series():
    r = requests.get("https://api.bls.gov/publicAPI/v2/surveys").json()
    results = r['Results']
    survey = results['survey']
    df = pd.DataFrame(survey)
    df.to_csv('survey_dict.csv', index=False)
    # Initialize an empty list to store (survey_name, series_id) tuples
    survey_series_list = []

    # Iterate through the survey abbreviations to fetch series IDs
    for index, row in df.iterrows():
        survey_abbreviation = row['survey_abbreviation']
        survey_name = row['survey_name']

        url = f"https://api.bls.gov/publicAPI/v2/timeseries/popular?survey={survey_abbreviation}"
        r = requests.get(url).json()
        try:
            series_ids = [item['seriesID'] for item in r.get('Results', {}).get('series', [])]
            print(series_ids)
            for series_id in series_ids:
                survey_series_list.append((survey_name, series_id))
        except TypeError:
            continue

    df2 = pd.DataFrame(survey_series_list)

    df2.to_csv('all_survey_series.csv', index=False)
    print('dataframe of surveys successfully saved.')


# Read the saved CSV into a DataFrame
df_series_ids = pd.read_csv('all_survey_series.csv')

# Initialize an empty list to store data
data_list = []

# Iterate through the Series IDs to fetch data
for index, row in df_series_ids.iterrows():
    survey_name = row['Survey Name']
    series_id = row['Series ID']

    url = f"https://api.bls.gov/publicAPI/v2/timeseries/data/{series_id}"
    r = requests.get(url).json()

    results = r['Results'] if 'Results' in r else None
    if results is not None:
        series = results['series'] if 'series' in results else None
        if series is not None:
            data = [i['data'] if 'data' in i else None for i in series]

            year = [i[0].get('year') for i in data]
            period = [i[0].get('period') for i in data]
            period_name = [i[0].get('periodName') for i in data]
            value = [i[0].get('value') for i in data]


            data_dict = { 
                'survey': survey_name,
                'series': series_id,
                'year': year[0],
                'period':period[0],
                'period_name': period_name[0],
                'value': value[0]
            }

            data_list.append(data_dict)


    df = pd.DataFrame(data_list)

    df.to_csv('all_economic_data.csv', index=False)

