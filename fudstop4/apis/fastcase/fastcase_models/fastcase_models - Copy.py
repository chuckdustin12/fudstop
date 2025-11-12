import pandas as pd




class DocumentDetails:
    def __init__(self, data):


        self.LibraryTypeNew = data.get('LibraryTypeNew', None)
        self.CitedIds = data.get('CitedIds', None)
        self.IsHistorical = data.get('IsHistorical', None)
        self.Date = data.get('Date', None)
        self.AuthorityLevel = data.get('AuthorityLevel', None)
        self.IdentifyingCitations = data.get('IdentifyingCitations', None)
        self.UniversalFilter = data.get('UniversalFilter', None)
        self.CitedGenerally = data.get('CitedGenerally', None)
        self.FullCitation = data.get('FullCitation', None)
        self.JurisdictionCourt = data.get('JurisdictionCourt', None)
        self.LibraryTypeNewCategory = data.get('LibraryTypeNewCategory', None)
        self.UniversalId = data.get('UniversalId', None)
        self.FastcaseId = data.get('FastcaseId', None)
        self.JurisdictionNew = data.get('JurisdictionNew', None)
        self.IsSlipOpinion = data.get('IsSlipOpinion', None)


        self.data_dict = { 
            'date': self.Date,
            'authority_level': self.AuthorityLevel.get('Name'),
            'cited_ids': ','.join(str(id) for id in self.CitedIds),
            'cited_generally': self.CitedGenerally,
            'full_citation': self.FullCitation,
            'universal_id': self.UniversalId,
            'identifying_citations': ','.join(str(id) for id in self.IdentifyingCitations)

        }


        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])