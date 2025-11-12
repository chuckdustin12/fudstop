import requests
import pandas as pd

def fetch_results(query):
    response = requests.get(f"https://www.ecfr.gov/api/search/v1/results?query={query}&per_page=20&page=1&order=relevance")
    data = response.json()
    return data

def flatten_data(data):
    flattened_data = {}
    for key, value in data.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flattened_data[f"{key}_{sub_key}"] = sub_value
        elif isinstance(value, list):
            for item in value:
                flattened_data[f"{key}_{item}"] = True
        else:
            flattened_data[key] = value
    return flattened_data

def create_dataframe(results):
    all_flattened_data = []
    for result in results:
        flattened_data = flatten_data(result)
        all_flattened_data.append(flattened_data)
    df = pd.DataFrame(all_flattened_data)
    return df

# Example usage:
results = fetch_results("options")
if 'results' in results:
    results = results['results']
    df = create_dataframe(results)
    df.to_csv('cfr_results_options.csv', index=False)
    print(df.columns)


