import pandas as pd
import httpx




class CourtListener:
    def __init__(self):
        self.token = "6a5e7823ec4cc5a2abf07db5f05270c138f15c9f"
        self.headers = {'Authorization': f'{self.token}', 'accept': 'application/json'}
        self.endpoints =       {
    "search": "https://www.courtlistener.com/api/rest/v4/search/",
    "dockets": "https://www.courtlistener.com/api/rest/v4/dockets/",
    "originating-court-information": "https://www.courtlistener.com/api/rest/v4/originating-court-information/",
    "docket-entries": "https://www.courtlistener.com/api/rest/v4/docket-entries/",
    "recap-documents": "https://www.courtlistener.com/api/rest/v4/recap-documents/",
    "courts": "https://www.courtlistener.com/api/rest/v4/courts/",
    "audio": "https://www.courtlistener.com/api/rest/v4/audio/",
    "clusters": "https://www.courtlistener.com/api/rest/v4/clusters/",
    "opinions": "https://www.courtlistener.com/api/rest/v4/opinions/",
    "opinions-cited": "https://www.courtlistener.com/api/rest/v4/opinions-cited/",
    "tag": "https://www.courtlistener.com/api/rest/v4/tag/",
    "people": "https://www.courtlistener.com/api/rest/v4/people/",
    "disclosure-typeahead": "https://www.courtlistener.com/api/rest/v4/disclosure-typeahead/",
    "positions": "https://www.courtlistener.com/api/rest/v4/positions/",
    "retention-events": "https://www.courtlistener.com/api/rest/v4/retention-events/",
    "educations": "https://www.courtlistener.com/api/rest/v4/educations/",
    "schools": "https://www.courtlistener.com/api/rest/v4/schools/",
    "political-affiliations": "https://www.courtlistener.com/api/rest/v4/political-affiliations/",
    "sources": "https://www.courtlistener.com/api/rest/v4/sources/",
    "aba-ratings": "https://www.courtlistener.com/api/rest/v4/aba-ratings/",
    "parties": "https://www.courtlistener.com/api/rest/v4/parties/",
    "attorneys": "https://www.courtlistener.com/api/rest/v4/attorneys/",
    "recap": "https://www.courtlistener.com/api/rest/v4/recap/",
    "recap-email": "https://www.courtlistener.com/api/rest/v4/recap-email/",
    "recap-fetch": "https://www.courtlistener.com/api/rest/v4/recap-fetch/",
    "recap-query": "https://www.courtlistener.com/api/rest/v4/recap-query/",
    "fjc-integrated-database": "https://www.courtlistener.com/api/rest/v4/fjc-integrated-database/",
    "tags": "https://www.courtlistener.com/api/rest/v4/tags/",
    "docket-tags": "https://www.courtlistener.com/api/rest/v4/docket-tags/",
    "prayers": "https://www.courtlistener.com/api/rest/v4/prayers/",
    "visualizations/json": "https://www.courtlistener.com/api/rest/v4/visualizations/json/",
    "visualizations": "https://www.courtlistener.com/api/rest/v4/visualizations/",
    "agreements": "https://www.courtlistener.com/api/rest/v4/agreements/",
    "debts": "https://www.courtlistener.com/api/rest/v4/debts/",
    "financial-disclosures": "https://www.courtlistener.com/api/rest/v4/financial-disclosures/",
    "gifts": "https://www.courtlistener.com/api/rest/v4/gifts/",
    "investments": "https://www.courtlistener.com/api/rest/v4/investments/",
    "non-investment-incomes": "https://www.courtlistener.com/api/rest/v4/non-investment-incomes/",
    "disclosure-positions": "https://www.courtlistener.com/api/rest/v4/disclosure-positions/",
    "reimbursements": "https://www.courtlistener.com/api/rest/v4/reimbursements/",
    "spouse-incomes": "https://www.courtlistener.com/api/rest/v4/spouse-incomes/",
    "alerts": "https://www.courtlistener.com/api/rest/v4/alerts/",
    "docket-alerts": "https://www.courtlistener.com/api/rest/v4/docket-alerts/",
    "memberships": "https://www.courtlistener.com/api/rest/v4/memberships/",
    "citation-lookup": "https://www.courtlistener.com/api/rest/v4/citation-lookup/"
}
        

    async def fetch_all_pages(self, endpoint):
        all_data = []  # To accumulate results from each page
        
        async with httpx.AsyncClient(headers=self.headers) as client:
            while endpoint:
                response = await client.get(endpoint)
                data = response.json()
                results = data['results']
                all_data.extend(results)  # Add results to the main list
                endpoint = data.get('next')  # Update to next page URL or None if at the end
        
        # Convert accumulated data to a DataFrame
        final_df = pd.DataFrame(all_data)
        
        print(final_df)
        return final_df
            