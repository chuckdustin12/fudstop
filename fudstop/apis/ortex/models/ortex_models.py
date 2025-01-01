import pandas as pd
import httpx

class ShortScores:
    def __init__(self, data):

        self.ticker = [i.get('ticker') for i in data]
        self.score = [i.get('score') for i in data]
        self.stock_id = [i.get('stock_id') for i in data]
        self.company_id = [i.get('company_id') for i in data]
        self.company = [i.get('company') for i in data]


        self.data_dict = { 
            'ticker':self.ticker,
            'score': self.score,
            'stock_id': self.stock_id,
            'company_id': self.company_id,
            'company': self.company

        }

        self.as_dataframe = pd.DataFrame(self.data_dict)



class Events:
    def __init__(self, data):
        self.type = [i.get('type') for i in data]
        self.epoch_time = [i.get('epoch_time') for i in data]
        self.event_type = [i.get('event_type') for i in data]
        self.event_sub_type = [i.get('event_sub_type') for i in data]
        self.title = [i.get('title') for i in data]
        self.country_iso = [i.get('country_iso') for i in data]

        self.data_dict = { 
            'type': self.type,
            'epoch_time': self.epoch_time,
            'event_type': self.event_type,
            'event_subtype': self.event_sub_type,
            'title': self.title,
            'country_iso': self.country_iso
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)


class Options:
    def __init__(self, data):
        self.ticker = [i.get('ticker') for i in data]
        self.stock_id = [i.get('stock_id') for i in data]
        self.name = [i.get('name') for i in data]
        self.positive_bets = [i.get('positive_bets') for i in data]
        self.negative_bets = [i.get('negative_bets') for i in data]
        self.sum_bets = [i.get('sum_bets') for i in data]
        self.positive_pc = [i.get('positive_pc') for i in data]
        self.negative_count = [i.get('negative_count') for i in data]
        self.positive_count = [i.get('positive_count') for i in data]
        self.positive_count_pc = [i.get('positive_count_pc') for i in data]
        self.exchangesymbol = [i.get('exchangesymbol') for i in data]
        self.date = [i.get('date') for i in data]
        self.score = [i.get('score') for i in data]


        self.data_dict = { 
            'ticker': self.ticker,
            'stock_id': self.stock_id,
            'name': self.name,
            'positive_bets': self.positive_bets,
            'negative_bets': self.negative_bets,
            'sum_bets': self.sum_bets,
            'positive_pc': self.positive_pc,
            'negative_count': self.negative_count,
            'positive_count': self.positive_count,
            'positive_count_pc': self.positive_count_pc,
            'exchange_sym': self.exchangesymbol,
            'date': self.date,
            'score': self.score
        }
        self.as_dataframe = pd.DataFrame(self.data_dict)
class Signals:
    def __init__(self, data):
        self.ticker = [i.get('ticker') for i in data]
        self.company_id = [i.get('company_id') for i in data]
        self.stock_id = [i.get('stock_id') for i in data]
        self.bs = [i.get('b/s') for i in data]
        self.desc = [i.get('desc') for i in data]
        self.type = [i.get('type') for i in data]
        self.company = [i.get('company') for i in data]
        self.exchangesymbol = [i.get('exchangesymbol') for i in data]


        self.data_dict = { 
            'ticker': self.ticker,
            'company_id': self.company_id,
            'stock_id': self.stock_id,
            'bs': self.bs,
            'desc': self.desc,
            'type': self.type,
            'company': self.company,
            'exchange_sym': self.exchangesymbol
        }
        self.as_dataframe = pd.DataFrame(self.data_dict)





class ShortInterestRows:
    def __init__(self, data):

        self.id = [i.get('id') for i in data]
        self.stock = [i.get('stock') for i in data]
        self.sector = [i.get('sector') for i in data]
        self.industry = [i.get('industry') for i in data]
        self.dtc_change = [i.get('dtc change') for i in data]
        self.cost_to_borrow_change = [i.get('cost to borrow change') for i in data]
        self.pctFFonloanchange = [i.get('% freefloat on loan change') for i in data]
        self.stock_id = [i.get('stock_id') for i in data]
        self.ticker = [i.get('ticker') for i in data]


        self.data_dict = { 
            'id': self.id,
            'stock': self.stock,
            'sector': self.sector,
            'industry': self.industry,
            'dtc_change': self.dtc_change,
            'cost_to_borrow_change': self.cost_to_borrow_change,
            'pctFFonloanchange': self.pctFFonloanchange,
            'stock_id': self.stock_id,
            'ticker': self.ticker
        }
        self.as_dataframe = pd.DataFrame(self.data_dict)

class ShortInterestColumns:
    def __init__(self, data):
        self.id = [i.get('id') for i in data]
        self.stock = [i.get('stock') for i in data]
        self.sector = [i.get('sector') for i in data]
        self.industry = [i.get('industry') for i in data]
        self.dtc_change = [i.get('dtc change') for i in data]
        self.cost_to_borrow_change = [i.get('cost to borrow change') for i in data]
        self.pctFFonloanchange = [i.get('% freefloat on loan change') for i in data]
        self.stock_id = [i.get('stock_id') for i in data]
        self.ticker = [i.get('ticker') for i in data]
        self.headerName = [i.get('headerName') for i in data]
        self.format = [i.get('format') for i in data]
        self.order = [i.get('order') for i in data]
        self.field = [i.get('field') for i in data]
        self.sortable = [i.get('sortable') for i in data]
        self.pinDirection = [i.get('pinDirection') for i in data]
        self.link = [i.get('link') for i in data]
        self.onClick = [i.get('onClick') for i in data]
        self.filterType = [i.get('filterType') for i in data]
        self.filterable = [i.get('filterable') for i in data]
        self.flex = [i.get('flex') for i in data]

        self.data_dict = { 
            'id': self.id,
            'stock': self.stock,
            'sector': self.sector,
            'industry': self.industry,
            'dtc_change': self.dtc_change,
            'cost_to_borrow_change': self.cost_to_borrow_change,
            'pctFFonloanchange': self.pctFFonloanchange,
            'stock_id': self.stock_id,
            'ticker': self.ticker,
            'headerName': self.headerName,
            'format': self.format,
            'order': self.order,
            'field': self.field,
            'sortable': self.sortable,
            'pin_dir': self.pinDirection,
            'link': self.link,
            'on_click': self.onClick,
            'filter_type': self.filterType,
            'filter_table': self.filterable,
            'flex': self.flex
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)




class NewsRows:
    def __init__(self, data):
        
        self.publishedAt = [i.get('publishedAt') for i in data]
        self.publishedDatetime = [i.get('publishedDatetime') for i in data]
        self.priority = [i.get('priority') for i in data]
        self.newsId = [i.get('newsId') for i in data]
        self.title = [i.get('title') for i in data]
        self.newsImportance = [i.get('newsImportance') for i in data]
        self.impact = [i.get('impact') for i in data]
        self.source = [i.get('source') for i in data]
        self.publisher = [i.get('publisher') for i in data]
        self.marketType = [i.get('marketType') for i in data]
        self.stock = [i.get('stock') for i in data]
        self.id = [i.get('id') for i in data]

        self.data_dict = {
            'published_at': self.publishedAt,
            'publish_date': self.publishedDatetime,
            'priority': self.priority,
            'news_id': self.newsId,
            'title': self.title,
            'importance': self.newsImportance,
            'impact': self.impact,
            'source': self.source,
            'publisher': self.publisher,
            'market_type': self.marketType,
            'stock': self.stock,
            'id': self.id
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)



class NewsColumns:
    def __init__(self, data):


        self.order = [i.get('order') for i in data]
        self.field = [i.get('field') for i in data]
        self.headerName = [i.get('headerName') for i in data]
        self.format = [i.get('format') for i in data]
        self.sortable = [i.get('sortable') for i in data]
        self.filterable = [i.get('filterable') for i in data]
        self.filterType = [i.get('filterType') for i in data]
        self.defaultSorter = [i.get('defaultSorter') for i in data]
        self.rangeMax = [i.get('rangeMax') for i in data]
        self.rangeMin = [i.get('rangeMin') for i in data]

        self.data_dict = {
            'order': self.order,
            'field': self.field,
            'header_name': self.headerName,
            'format': self.format,
            'sortable': self.sortable,
            'filterable': self.filterable,
            'filter_type': self.filterType,
            'default_sorter': self.defaultSorter,
            'rangeMax': self.rangeMax,
            'rangeMin': self.rangeMin
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)




class SectorShorts:
    def __init__(self, data):

        self.id = [i.get('id') for i in data]
        self.sector = [i.get('sector') for i in data]
        self.dtc = [i.get('dtc') for i in data]
        self.pct_on_loan = [i.get('%freefloat on loan') for i in data]
        self.gic = [i.get('gic') for i in data]


        self.data_dict = { 
            'id': self.id,
            'sector': self.sector,
            'dtc': self.dtc,
            'pct_on_loan': self.pct_on_loan,
            'gic': self.gic
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)
