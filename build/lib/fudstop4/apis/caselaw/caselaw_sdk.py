from .caselaw_models import CaseData, CaseBody
import requests


import pandas as pd
import aiohttp
class CaseLawSDK:
    def __init__(self):
        pass




    async def get_cases(self):
        counter = 1
        all_cases = []
        async with aiohttp.ClientSession() as session:
            while True:
                endpoint = f"https://static.case.law/tex-civ-app/{counter}/CasesMetadata.json"
                print(endpoint)
                try:
                    async with session.get(endpoint) as response:
                        if response.status != 200:
                            break
                        data = await response.json()
                        data_dict = CaseData(data).data_dict
                        data_dict.update({'volume': counter})
                        all_cases.append(data_dict)
                except Exception as e:
                    print(f"Failed to get data from {endpoint}: {str(e)}")
                    break
                counter += 1
        flattened_cases = [{key: value for key, value in zip(case.keys(), item)} 
                        for case in all_cases for item in zip(*case.values())]
        df = pd.DataFrame(flattened_cases)

        return df



    async def case_details(self, file_name):
        endpoint = f"https://static.case.law/tex-civ-app/2/cases/{file_name}.json"

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(endpoint) as response:
                    data = await response.json()
                    print(CaseBody(data).author)



            except Exception as e:
                print(e)