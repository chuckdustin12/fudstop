import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from fudstop4.apis.helpers import format_large_numbers_in_dataframe2
class CapitalFlow:
    """
    A class representing capital flow data for a stock.

    Attributes:
        super_in (float): The amount of super large inflow formatted with commas.
        super_out (float): The amount of super large outflow formatted with commas.
        super_net (float): The amount of super large net flow formatted with commas.
        large_in (float): The amount of large inflow formatted with commas.
        large_out (float): The amount of large outflow formatted with commas.
        large_net (float): The amount of large net flow formatted with commas.
        new_large_in (float): The amount of new large inflow formatted with commas.
        new_large_out (float): The amount of new large outflow formatted with commas.
        new_large_net (float): The amount of new large net flow formatted with commas.
        new_large_in_ratio (float): The new large inflow ratio formatted as a percentage with 2 decimal places.
        new_large_out_ratio (float): The new large outflow ratio formatted as a percentage with 2 decimal places.
        medium_in (float): The amount of medium inflow formatted with commas.
        medium_out (float): The amount of medium outflow formatted with commas.
        medium_net (float): The amount of medium net flow formatted with commas.
        medium_in_ratio (float): The medium inflow ratio formatted as a percentage with 2 decimal places.
        medium_out_ratio (float): The medium outflow ratio formatted as a percentage with 2 decimal places.
        small_in (float): The amount of small inflow formatted with commas.
        small_out (float): The amount of small outflow formatted with commas.
        small_net (float): The amount of small net flow formatted with commas.
        small_in_ratio (float): The small inflow ratio formatted as a percentage with 2 decimal places.
        small_out_ratio (float): The small outflow ratio formatted as a percentage with 2 decimal places.
        major_in (float): The amount of major inflow formatted with commas.
        major_in_ratio (float): The major inflow ratio formatted as a percentage with 2 decimal places.
        major_out (float): The amount of major outflow formatted with commas.
        major_out_ratio (float): The major outflow ratio formatted as a percentage with 2 decimal places.
        major_net (float): The amount of major net flow formatted with commas.
        retail_in (float): The amount of retail inflow formatted with commas.
        retail_in_ratio (float): The retail inflow ratio formatted as a percentage with 2 decimal places.
        retail_out (float): The amount of retail outflow formatted with commas.
        retail_out_ratio (float): The retail outflow ratio formatted as a percentage with 2 decimal places.

    Methods:
        async def capital_flow(id: str) -> CapitalFlow:
            Returns an instance of the CapitalFlow class for a given stock ticker ID.
            The data is fetched asynchronously using aiohttp.
    """

    def __init__(self, item, ticker):

        self.superLargeInflow = float(item.get('superLargeInflow', 0))
        self.superLargeOutflow = float(item.get('superLargeOutflow', 0))
        self.superLargeNetFlow = float(item.get('superLargeNetFlow', 0))
        self.largeInflow = float(item.get('largeInflow', 0))
        self.largeOutflow = float(item.get('largeOutflow', 0))
        self.largeNetFlow = float(item.get('largeNetFlow', 0))
        self.newLargeInflow = float(item.get('newLargeInflow', 0))
        self.newLargeOutflow = float(item.get('newLargeOutflow', 0))
        self.newLargeNetFlow = float(item.get('newLargeNetFlow', 0))
        self.newLargeInflowRatio = float(item.get('newLargeInflowRatio', 0))
        self.newLargeOutflowRatio = float(item.get('newLargeOutflowRatio', 0))
        self.mediumInflow = float(item.get('mediumInflow', 0))
        self.mediumOutflow = float(item.get('mediumOutflow', 0))
        self.mediumNetFlow = float(item.get('mediumNetFlow', 0))
        self.mediumInflowRatio = float(item.get('mediumInflowRatio', 0))
        self.mediumOutflowRatio = float(item.get('mediumOutflowRatio', 0))
        self.smallInflow = float(item.get('smallInflow', 0))
        self.smallOutflow = float(item.get('smallOutflow', 0))
        self.smallNetFlow = float(item.get('smallNetFlow', 0))
        self.smallInflowRatio = float(item.get('smallInflowRatio', 0))
        self.smallOutflowRatio = float(item.get('smallOutflowRatio', 0))
        self.majorInflow = float(item.get('majorInflow', 0))
        self.majorInflowRatio = float(item.get('majorInflowRatio', 0))
        self.majorOutflow = float(item.get('majorOutflow', 0))
        self.majorOutflowRatio = float(item.get('majorOutflowRatio', 0))
        self.majorNetFlow = float(item.get('majorNetFlow', 0))
        self.retailInflow = float(item.get('retailInflow', 0))
        self.retailInflowRatio = float(item.get('retailInflowRatio', 0))
        self.retailOutflow = float(item.get('retailOutflow', 0))
        self.retailOutflowRatio = float(item.get('retailOutflowRatio', 0))

        self.data_dict = {
            'ticker': ticker,
            'large_inflow': self.largeInflow,
            'large_outflow': self.largeOutflow,
            'large_net_flow': self.largeNetFlow,
            'new_large_inflow': self.newLargeInflow,
            'new_large_outflow': self.newLargeOutflow,
            'new_large_net_flow': self.newLargeNetFlow,
            'new_large_inflow_ratio': self.newLargeInflowRatio,
            'new_large_outflow_ratio': self.newLargeOutflowRatio,
            'medium_inflow': self.mediumInflow,
            'medium_outflow': self.mediumOutflow,
            'medium_net_flow': self.mediumNetFlow,
            'medium_inflow_ratio': self.mediumInflowRatio,
            'medium_outflow_ratio': self.mediumOutflowRatio,
            'small_inflow': self.smallInflow,
            'small_outflow': self.smallOutflow,
            'small_net_flow': self.smallNetFlow,
            'small_inflow_ratio': self.smallInflowRatio,
            'small_outflow_ratio': self.smallOutflowRatio,
            'major_inflow': self.majorInflow,
            'major_inflow_ratio': self.majorInflowRatio,
            'major_outflow': self.majorOutflow,
            'major_outflow_ratio': self.majorOutflowRatio,
            'major_net_flow': self.majorNetFlow,
            'retail_inflow': self.retailInflow,
            'retail_inflow_ratio': self.retailInflowRatio,
            'retail_outflow': self.retailOutflow,
            'retail_outflow_ratio': self.retailOutflowRatio
        }
        self.df = pd.DataFrame(self.data_dict, index=[0])
        self.df = format_large_numbers_in_dataframe2(self.df)
        self.df = self.df[::-1]
        


class CapitalFlowHistory:
    """
    A class representing capital flow data for a stock.

    Attributes:
        superin (list): List of super large inflow values.
        superout (list): List of super large outflow values.
        supernet (list): List of super large net flow values.
        largein (list): List of large inflow values.
        largeout (list): List of large outflow values.
        largenet (list): List of large net flow values.
        newlargein (list): List of new large inflow values.
        newlargeout (list): List of new large outflow values.
        newlargenet (list): List of new large net flow values.
        newlargeinratio (list): List of new large inflow ratios as percentages.
        newlargeoutratio (list): List of new large outflow ratios as percentages.
        mediumin (list): List of medium inflow values.
        mediumout (list): List of medium outflow values.
        mediumnet (list): List of medium net flow values.
        mediuminratio (list): List of medium inflow ratios as percentages.
        mediumoutratio (list): List of medium outflow ratios as percentages.
        smallin (list): List of small inflow values.
        smallout (list): List of small outflow values.
        smallnet (list): List of small net flow values.
        smallinratio (list): List of small inflow ratios as percentages.
        smalloutratio (list): List of small outflow ratios as percentages.
        majorin (list): List of major inflow values.
        majorinratio (list): List of major inflow ratios as percentages.
        majorout (list): List of major outflow values.
        majoroutratio (list): List of major outflow ratios as percentages.
        majornet (list): List of major net flow values.
        retailin (list): List of retail inflow values.
        retailinratio (list): List of retail inflow ratios as percentages.
        retailout (list): List of retail outflow values.
        retailoutratio (list): List of retail outflow ratios as percentages.
    """

    def __init__(self, item, ticker):
        self.ticker=ticker
        self.superLargeInflow = [float(i.get('superLargeInflow',0)) for i in item]
        self.superLargeOutflow = [float(i.get('superLargeOutflow',0)) for i in item]
        self.superLargeNetFlow = [float(i.get('superLargeNetFlow',0)) for i in item]
        self.largeInflow = [float(i.get('largeInflow',0)) for i in item]
        self.largeOutflow = [float(i.get('largeOutflow',0)) for i in item]
        self.largeNetFlow = [float(i.get('largeNetFlow',0)) for i in item]
        self.newLargeInflow = [float(i.get('newLargeInflow',0)) for i in item]
        self.newLargeOutflow = [float(i.get('newLargeOutflow',0)) for i in item]
        self.newLargeNetFlow = [float(i.get('newLargeNetFlow',0)) for i in item]
        self.newLargeInflowRatio = [float(i.get('newLargeInflowRatio',0)) for i in item]
        self.newLargeOutflowRatio = [float(i.get('newLargeOutflowRatio',0)) for i in item]
        self.mediumInflow = [float(i.get('mediumInflow',0)) for i in item]
        self.mediumOutflow = [float(i.get('mediumOutflow',0)) for i in item]
        self.mediumNetFlow = [float(i.get('mediumNetFlow',0)) for i in item]
        self.mediumInflowRatio = [float(i.get('mediumInflowRatio',0)) for i in item]
        self.mediumOutflowRatio = [float(i.get('mediumOutflowRatio',0)) for i in item]
        self.smallInflow = [float(i.get('smallInflow',0)) for i in item]
        self.smallOutflow = [float(i.get('smallOutflow',0)) for i in item]
        self.smallNetFlow = [float(i.get('smallNetFlow',0)) for i in item]
        self.smallInflowRatio = [float(i.get('smallInflowRatio',0)) for i in item]
        self.smallOutflowRatio = [float(i.get('smallOutflowRatio',0)) for i in item]
        self.majorInflow = [float(i.get('majorInflow',0)) for i in item]
        self.majorInflowRatio = [float(i.get('majorInflowRatio',0)) for i in item]
        self.majorOutflow = [float(i.get('majorOutflow',0)) for i in item]
        self.majorOutflowRatio = [float(i.get('majorOutflowRatio',0)) for i in item]
        self.majorNetFlow = [float(i.get('majorNetFlow',0)) for i in item]
        self.retailInflow = [float(i.get('retailInflow',0)) for i in item]
        self.retailInflowRatio = [float(i.get('retailInflowRatio',0)) for i in item]
        self.retailOutflow = [float(i.get('retailOutflow',0)) for i in item]
        self.retailOutflowRatio = [float(i.get('retailOutflowRatio',0)) for i in item]

        self.data_dict = {
            'ticker': self.ticker,
            'large_inflow': self.largeInflow,
            'large_outflow': self.largeOutflow,
            'large_net_flow': self.largeNetFlow,
            'new_large_inflow': self.newLargeInflow,
            'new_large_outflow': self.newLargeOutflow,
            'new_large_net_flow': self.newLargeNetFlow,
            'new_large_inflow_ratio': self.newLargeInflowRatio,
            'new_large_outflow_ratio': self.newLargeOutflowRatio,
            'medium_inflow': self.mediumInflow,
            'medium_outflow': self.mediumOutflow,
            'medium_net_flow': self.mediumNetFlow,
            'medium_inflow_ratio': self.mediumInflowRatio,
            'medium_outflow_ratio': self.mediumOutflowRatio,
            'small_inflow': self.smallInflow,
            'small_outflow': self.smallOutflow,
            'small_net_flow': self.smallNetFlow,
            'small_inflow_ratio': self.smallInflowRatio,
            'small_outflow_ratio': self.smallOutflowRatio,
            'major_inflow': self.majorInflow,
            'major_inflow_ratio': self.majorInflowRatio,
            'major_outflow': self.majorOutflow,
            'major_outflow_ratio': self.majorOutflowRatio,
            'major_net_flow': self.majorNetFlow,
            'retail_inflow': self.retailInflow,
            'retail_inflow_ratio': self.retailInflowRatio,
            'retail_outflow': self.retailOutflow,
            'retail_outflow_ratio': self.retailOutflowRatio
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)
        self.as_dataframe = self.as_dataframe[::-1]
        start_date = datetime.now() - timedelta(days=1)  # Yesterday

        # Create a new column with dates decreasing by 1 day for each row
        self.as_dataframe['date'] = [start_date - timedelta(days=i) for i in range(len(self.as_dataframe))]

        # Format the date to yyyy-mm-dd
        self.as_dataframe['date'] = self.as_dataframe['date'].dt.strftime('%Y-%m-%d')
