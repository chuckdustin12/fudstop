import aiohttp
import pandas as pd
class CapitalFlow:
    """
    A class representing capital flow data for a stock.

    Attributes:
        superin (float): The amount of super large inflow formatted with commas.
        superout (float): The amount of super large outflow formatted with commas.
        supernet (float): The amount of super large net flow formatted with commas.
        largein (float): The amount of large inflow formatted with commas.
        largeout (float): The amount of large outflow formatted with commas.
        largenet (float): The amount of large net flow formatted with commas.
        newlargein (float): The amount of new large inflow formatted with commas.
        newlargeout (float): The amount of new large outflow formatted with commas.
        newlargenet (float): The amount of new large net flow formatted with commas.
        newlargeinratio (float): The new large inflow ratio formatted as a percentage with 2 decimal places.
        newlargeoutratio (float): The new large outflow ratio formatted as a percentage with 2 decimal places.
        mediumin (float): The amount of medium inflow formatted with commas.
        mediumout (float): The amount of medium outflow formatted with commas.
        mediumnet (float): The amount of medium net flow formatted with commas.
        mediuminratio (float): The medium inflow ratio formatted as a percentage with 2 decimal places.
        mediumoutratio (float): The medium outflow ratio formatted as a percentage with 2 decimal places.
        smallin (float): The amount of small inflow formatted with commas.
        smallout (float): The amount of small outflow formatted with commas.
        smallnet (float): The amount of small net flow formatted with commas.
        smallinratio (float): The small inflow ratio formatted as a percentage with 2 decimal places.
        smalloutratio (float): The small outflow ratio formatted as a percentage with 2 decimal places.
        majorin (float): The amount of major inflow formatted with commas.
        majorinratio (float): The major inflow ratio formatted as a percentage with 2 decimal places.
        majorout (float): The amount of major outflow formatted with commas.
        majoroutratio (float): The major outflow ratio formatted as a percentage with 2 decimal places.
        majornet (float): The amount of major net flow formatted with commas.
        retailin (float): The amount of retail inflow formatted with commas.
        retailinratio (float): The retail inflow ratio formatted as a percentage with 2 decimal places.
        retailout (float): The amount of retail outflow formatted with commas.
        retailoutratio (float): The retail outflow ratio formatted as a percentage with 2 decimal places.

    Methods:
        async def capital_flow(id: str) -> CapitalFlow:
            Returns an instance of the CapitalFlow class for a given stock ticker ID.
            The data is fetched asynchronously using aiohttp.
    """

    def __init__(self, item):
        self.superin = float(item['superLargeInFlow']) if 'superLargeInFlow' in item else None
        self.superout = float(item['superLargeOutFlow']) if 'superLargeOutFlow' in item else None
        self.supernet = float(item['superLargeNetFlow']) if 'superLargeNetFlow' in item else None
        self.largein = float(item['largeInFlow']) if 'largeInFlow' in item else None
        self.largeout = float(item['largeOutFlow']) if 'largeOutFlow' in item else None
        self.largenet = float(item['largeNetFlow']) if 'largeNetFlow' in item else None
        self.newlargein = float(item['newLargeInFlow']) if 'newLargeInFlow' in item else None
        self.newlargeout = float(item['newLargeOutFlow']) if 'newLargeOutFlow' in item else None
        self.newlargenet = float(item['newLargeNetFlow']) if 'newLargeNetFlow' in item else None
        self.newlargeinratio = float(item['newLargeInFlowRatio']) if 'newLargeInFlowRatio' in item else None
        self.newlargeoutratio = float(item['newLargeOutFlowRatio']) if 'newLargeOutFlowRatio' in item else None
        self.mediumin = float(item['mediumInFlow']) if 'mediumInFlow' in item else None
        self.mediumout = float(item['mediumOutFlow']) if 'mediumOutFlow' in item else None
        self.mediumnet = float(item['mediumNetFlow']) if 'mediumNetFlow' in item else None
        self.mediuminratio = float(item['mediumInFlowRatio']) if 'mediumInFlowRatio' in item else None
        self.mediumoutratio = float(item['mediumOutFlowRatio']) if 'mediumOutFlowRatio' in item else None
        self.smallin = float(item['smallInFlow']) if 'smallInFlow' in item else None
        self.smallout = float(item['smallOutFlow']) if 'smallOutFlow' in item else None
        self.smallnet = float(item['smallNetFlow']) if 'smallNetFlow' in item else None
        self.smallinratio = float(item['smallInFlowRatio']) if 'smallInFlowRatio' in item else None
        self.smalloutratio = float(item['smallOutFlowRatio']) if 'smallOutFlowRatio' in item else None
        self.majorin = float(item['majorInFlow']) if 'majorInFlow' in item else None
        self.majorinratio = float(item['majorInFlowRatio']) if 'majorInFlowRatio' in item else None
        self.majorout = float(item['majorOutFlow']) if 'majorOutFlow' in item else None
        self.majoroutratio = float(item['majorOutFlowRatio']) if 'majorOutFlowRatio' in item else None
        self.majornet = float(item['majorNetFlow']) if 'majorNetFlow' in item else None
        self.retailin = float(item['retailInFlow']) if 'retailInFlow' in item else None
        self.retailinratio = float(item['retailInFlowRatio']) if 'retailInFlowRatio' in item else None
        self.retailout = float(item['retailOutFlow']) if 'retailOutFlow' in item else None
        self.retailoutratio = float(item['retailOutFlowRatio']) if 'retailOutFlowRatio' in item else None


        self.data_dict = {
            'superin': float(item['superLargeInFlow']) if 'superLargeInFlow' in item else None,
            'superout': float(item['superLargeOutFlow']) if 'superLargeOutFlow' in item else None,
            'supernet': float(item['superLargeNetFlow']) if 'superLargeNetFlow' in item else None,
            'largein': float(item['largeInFlow']) if 'largeInFlow' in item else None,
            'largeout': float(item['largeOutFlow']) if 'largeOutFlow' in item else None,
            'largenet': float(item['largeNetFlow']) if 'largeNetFlow' in item else None,
            'newlargein': float(item['newLargeInFlow']) if 'newLargeInFlow' in item else None,
            'newlargeout': float(item['newLargeOutFlow']) if 'newLargeOutFlow' in item else None,
            'newlargenet': float(item['newLargeNetFlow']) if 'newLargeNetFlow' in item else None,
            'newlargeinratio': float(item['newLargeInFlowRatio']) if 'newLargeInFlowRatio' in item else None,
            'newlargeoutratio': float(item['newLargeOutFlowRatio']) if 'newLargeOutFlowRatio' in item else None,
            'mediumin': float(item['mediumInFlow']) if 'mediumInFlow' in item else None,
            'mediumout': float(item['mediumOutFlow']) if 'mediumOutFlow' in item else None,
            'mediumnet': float(item['mediumNetFlow']) if 'mediumNetFlow' in item else None,
            'mediuminratio': float(item['mediumInFlowRatio']) if 'mediumInFlowRatio' in item else None,
            'mediumoutratio': float(item['mediumOutFlowRatio']) if 'mediumOutFlowRatio' in item else None,
            'smallin': float(item['smallInFlow']) if 'smallInFlow' in item else None,
            'smallout': float(item['smallOutFlow']) if 'smallOutFlow' in item else None,
            'smallnet': float(item['smallNetFlow']) if 'smallNetFlow' in item else None,
            'smallinratio': float(item['smallInFlowRatio']) if 'smallInFlowRatio' in item else None,
            'smalloutratio': float(item['smallOutFlowRatio']) if 'smallOutFlowRatio' in item else None,
            'majorin': float(item['majorInFlow']) if 'majorInFlow' in item else None,
            'majorinratio': float(item['majorInFlowRatio']) if 'majorInFlowRatio' in item else None,
            'majorout': float(item['majorOutFlow']) if 'majorOutFlow' in item else None,
            'majoroutratio': float(item['majorOutFlowRatio']) if 'majorOutFlowRatio' in item else None,
            'majornet': float(item['majorNetFlow']) if 'majorNetFlow' in item else None,
            'retailin': float(item['retailInFlow']) if 'retailInFlow' in item else None,
            'retailinratio': float(item['retailInFlowRatio']) if 'retailInFlowRatio' in item else None,
            'retailout': float(item['retailOutFlow']) if 'retailOutFlow' in item else None,
            'retailoutratio': float(item['retailOutFlowRatio']) if 'retailOutFlowRatio' in item else None
        }

        self.df = pd.DataFrame(self.data_dict, index=[0]).transpose()

    async def fetch_data(self, id):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/ticker?tickerId={id}&showHis=true") as response:
                r = await response.json()
                latest = r.get('latest', None)
                if latest is not None:
                    item = latest.get('item', None)
                    if item is not None:
                        self.superin = "{:,}".format(item.get('superLargeInflow', 0))
                        self.superout = "{:,}".format(item.get('superLargeOutflow', 0))
                        self.supernet = "{:,}".format(item.get('superLargeNetFlow', 0))
                        self.largein = "{:,}".format(item.get('largeInflow', 0))
                        self.largeout = "{:,}".format(item.get('largeOutflow', 0))
                        self.largenet = "{:,}".format(item.get('largeNetFlow', 0))
                        self.newlargein = "{:,}".format(item.get('newLargeInflow', 0))
                        self.newlargeout = "{:,}".format(item.get('newLargeOutflow', 0))
                        self.newlargenet = "{:,}".format(item.get('newLargeNetFlow', 0))
                        self.newlargeinratio = "{:,.2f}".format(float(item.get('newLargeInflowRatio', 0)) * 100)
                        self.newlargeoutratio = "{:,.2f}".format(float(item.get('newLargeOutflowRatio', 0)) * 100)
                        self.mediumin = "{:,}".format(item.get('mediumInflow', 0))
                        self.mediumout = "{:,}".format(item.get('mediumOutflow', 0))
                        self.mediumnet = "{:,}".format(item.get('mediumNetFlow', 0))
                        self.mediuminratio = "{:,.2f}".format(float(item.get('mediumInflowRatio', 0)) * 100)
                        self.mediumoutratio = "{:,.2f}".format(float(item.get('mediumOutflowRatio', 0)) * 100)
                        self.smallin = "{:,}".format(item.get('smallInflow', 0))
                        self.smallout = "{:,}".format(item.get('smallOutflow', 0))
                        self.smallnet = "{:,}".format(item.get('smallNetFlow', 0))
                        self.smallinratio = "{:,.2f}".format(float(item.get('smallInflowRatio', 0)) * 100)
                        self.smalloutratio = "{:,.2f}".format(float(item.get('smallOutflowRatio', 0)) * 100)
                        self.majorin = "{:,}".format(item.get('majorInflow', 0))
                        self.majorinratio = "{:,.2f}".format(float(item.get('majorInflowRatio', 0)) * 100)
                        self.majorout = "{:,}".format(item.get('majorOutflow', 0))
                        self.majoroutratio = "{:,.2f}".format(float(item.get('majorOutflowRatio', 0)) * 100)
                        self.majornet = "{:,}".format(item.get('majorNetFlow', 0))
                        self.retailin = "{:,}".format(item.get('retailInflow', 0))
                        self.retailinratio = "{:,.2f}".format(float(item.get('retailInflowRatio', 0)) * 100)
                        self.retailout = "{:,}".format(item.get('retailOutflow', 0))
                        self.retailoutratio = "{:,.2f}".format(float(item.get('retailOutflowRatio', 0)) * 100)