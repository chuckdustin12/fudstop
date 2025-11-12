import pandas as pd



class CaseData:
    def __init__(self, data):

        self.id = [i.get('id', '') for i in data]
        self.name = [i.get('name', '') for i in data]
        self.name_abbreviation = [i.get('name_abbreviation', '') for i in data]
        self.decision_date = [i.get('decision_date', '') for i in data]
        self.docket_number = [i.get('docket_number', '') for i in data]
        self.first_page = [i.get('first_page', '') for i in data]
        self.last_page = [i.get('last_page', '') for i in data]
        self.cites_to = [', '.join(cite.get('citation', '') for cite in i.get('cites_to', [])) for i in data]

        # Handling 'analysis' which can be None
        analysis = [i.get('analysis', {}) for i in data]  # Provide empty dict as default
        self.cardinality = [i.get('cardinality', 0) for i in analysis]
        self.char_count = [i.get('char_count', 0) for i in analysis]
        self.ocr_confidence = [i.get('ocr_confidence', 0) for i in analysis]

        # Handling 'pagerank' nested within 'analysis' which can also be None
        pagerank = [i.get('pagerank', {'raw': 0, 'percentile': 0.0}) for i in analysis]  # Provide default values in a dict
        self.raw = [i['raw'] for i in pagerank]
        self.percentile = [round(float(i['percentile']) * 100, 2) for i in pagerank]



        self.sha256 = [i.get('sha256') for i in analysis]
        self.simhash = [i.get('simhash') for i in analysis]
        self.word_count = [i.get('word_count') for i in analysis]


        self.last_updated = [i.get('last_updated') for i in data]
        self.file_name = [i.get('file_name') for i in data]
        self.first_page_order = [i.get('first_page_order') for i in data]
        self.last_page_order = [i.get('last_page_order') for i in data]

        self.data_dict = { 

            'id': self.id,
            'name': self.name,
            'name_abbv': self.name_abbreviation,
            'decision_date': self.decision_date,
            'docket_number': self.docket_number,
            'first_page': self.first_page,
            'last_page': self.last_page,
            'cites_to': self.cites_to,
            'cardinality': self.cardinality,
            'char_count': self.char_count,
            'ocr_confidence': self.ocr_confidence,
            'rank_raw': self.raw,
            'rank_percentile': self.percentile,
            'sha256': self.sha256,
            'simhash': self.simhash,
            'word_count': self.word_count,
            'last_updated': self.last_updated,
            'file_name': self.file_name

        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class CaseBody:
    def __init__(self, data):

        opinions = [i.get('opinions') for i in data]
        opinions = [item for sublist in opinions for item in sublist]
        self.author = [i.get('author') for i in opinions]
        self.text = [i.get('text') for i in opinions]
        self.type = [i.get('type') for i in opinions]



        self.data_dict = { 
            'author': self.author,
            'text': self.text,
            'type': self.type
        }