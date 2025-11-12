import pandas as pd





class PandasSDK:
    def __init__(self):

        pass



    def print_lengths(self, data_dict):
        for key, value in data_dict.items():
            print(f"{key}: {len(value)}")
