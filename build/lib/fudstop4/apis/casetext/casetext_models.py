import pandas as pd

import datetime
class CaseLawCases:
    def __init__(self, case_rows):
     

        self.citation_string = [i.get('citationString') for i in case_rows]
        self.citations = [','.join(i.get('citations')) for i in case_rows]
        citator = [i.get('citator') for i in case_rows]
        citator = [item for sublist in citator for item in sublist]
        self.citator_slug = [i.get('slug') for i in citator]
        self.treatment = [i.get('treatmentCategory') for i in citator]
        self.highlights = [str(','.join(i.get('highlights'))) for i in case_rows]
        # Example usage within your existing code
        self.dates = [i.get('dates', {}).get('decide') for i in case_rows]  # Extract the 'decide' timestamp
        self.dates = [self.parse_unix_timestamp_ms(d) for d in self.dates]  # Convert the timestamps to date strings

        paragraphs = [i.get('paragraphs') for i in case_rows]
        paragraph_rows = [i.get('rows') for i in paragraphs if i is not None]
        paragraph_rows = [item for sublist in paragraph_rows for item in sublist]
        self.paragraph_number = [i.get('paragraphNumber') for i in paragraph_rows]
        self.page_number = [i.get('page') for i in paragraph_rows]
        self.paragraph_text = [i.get('text') for i in paragraph_rows]

        summaries = [i.get('summaries') for i in case_rows]
        summary_rows = [i.get('rows') for i in summaries]
        summary_rows = [item for sublist in summary_rows for item in sublist]
        self.summary_citation = [i.get('citationString') for i in summary_rows]
        self.summary_text = [i.get('text') for i in summary_rows]
        self.summary_slug = [i.get('slug') for i in summary_rows]
        self.summary_title = [i.get('title') for i in summary_rows]
        self.slug = [i.get('slug') for i in summary_rows]


        self.data_dict = { 
            'citation_string': self.citation_string,
            'citations': self.citations,
            'citator_slug': self.citator_slug,
            'treatment': self.treatment,
            'highlights': self.highlights,
            'summary_citation': self.summary_citation,
            'summary_slug': self.summary_slug,
            'summary_title': self.summary_title,
            'summary_text': self.summary_text,
            'paragraph_number': self.paragraph_number,
            'paragraph_page': self.page_number,
            'paragraph_text': self.paragraph_text,
            'slug': self.slug,
            'date': self.dates
        }


        self._equalize_lengths()
        self.as_dataframe = pd.DataFrame(self.data_dict)
        self.as_dataframe = self.as_dataframe.dropna(subset=['slug'])
        self.as_dataframe['paragraph_text'] = self.as_dataframe['paragraph_text'].str.replace(r'<\/?em>', '', regex=True)
    def _equalize_lengths(self):
        max_length = max(len(lst) for lst in self.data_dict.values())
        for key in self.data_dict:
            current_length = len(self.data_dict[key])
            if current_length < max_length:
                self.data_dict[key].extend([None] * (max_length - current_length))

        self.as_dataframe = pd.DataFrame(self.data_dict)


    def parse_unix_timestamp_ms(self, timestamp_ms):
        """Convert a Unix timestamp in milliseconds to 'YYYY-MM-DD' format string."""
        try:
            if isinstance(timestamp_ms, int) and timestamp_ms > 0:
                # Check if it's a reasonable Unix timestamp (after Jan 1, 1970 and not far in the future)
                return datetime.datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d')
            else:
                # Handle invalid timestamps (e.g., negative or non-integer)
                return None
        except (OSError, OverflowError, ValueError) as e:
            # Catch any errors related to invalid timestamp values
            print(f"Error parsing timestamp: {timestamp_ms} -> {e}")
            return None




class CaseLawBriefs:
    def __init__(self, brief_rows):
        
        self.description = [i.get('description') for i in brief_rows]
        self.document = [i.get('document') for i in brief_rows]
        self.date = [i.get('date') for i in brief_rows]
        self.citation_string = [i.get('citationString') for i in brief_rows]
        self.case_name = [i.get('caseName') for i in brief_rows]
        self.nature_of_suit = [i.get('natureOfSuit') for i in brief_rows]
        self.highlights = [str(','.join(i.get('highlights'))) for i in brief_rows]
        self.slug = [i.get('slug') for i in brief_rows]


        self.data_dict = { 
            'date': self.date,
            'document': self.document,
            'case_name':self.case_name,
            'citation_string': self.citation_string,
            'highlights': self.highlights,
            'nature_of_suit': self.nature_of_suit,
            'slug': self.slug,


        }


        self._equalize_lengths()
        self.as_dataframe = pd.DataFrame(self.data_dict)

    def _equalize_lengths(self):
        max_length = max(len(lst) for lst in self.data_dict.values())
        for key in self.data_dict:
            current_length = len(self.data_dict[key])
            if current_length < max_length:
                self.data_dict[key].extend([None] * (max_length - current_length))

        self.as_dataframe = pd.DataFrame(self.data_dict)


class CaseLawRules:
    def __init__(self, rule_rows):
        

        self.citation_string = [i.get('citationString') for i in rule_rows]
        self.highlights = [str(','.join(i.get('highlights'))) for i in rule_rows]
        self.slug = [i.get('slug') for i in rule_rows]
        self.citations = [','.join(i.get('citations')) for i in rule_rows]
        self.title = [i.get('title') for i in rule_rows]

        self.data_dict = { 
            'title': self.title,
            'citations': self.citations,
            'citation_string': self.citation_string,
            'highlights': self.highlights,
            'slug': self.slug,


        }


        self._equalize_lengths()
        self.as_dataframe = pd.DataFrame(self.data_dict)

    def _equalize_lengths(self):
        max_length = max(len(lst) for lst in self.data_dict.values())
        for key in self.data_dict:
            current_length = len(self.data_dict[key])
            if current_length < max_length:
                self.data_dict[key].extend([None] * (max_length - current_length))

        self.as_dataframe = pd.DataFrame(self.data_dict)