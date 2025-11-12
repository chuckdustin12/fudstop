from typing import Any, Dict, List
import pandas as pd

class TickerNews:
    def __init__(self, results: List[Dict[str, Any]]):
        # Initialize empty lists for each field
        amp_urls = []
        article_urls = []
        authors = []
        descriptions = []
        ids = []
        image_urls = []
        keywords_list = []
        tickers_list = []
        titles = []
        favicons = []
        names = []
        logo_urls = []
        homepage_urls = []

        # Process each article once and extract the fields
        for article in results:
            amp_urls.append(article.get("amp_url"))
            article_urls.append(article.get("article_url"))
            authors.append(article.get("author"))
            descriptions.append(article.get("description"))
            ids.append(article.get("id"))
            image_urls.append(article.get("image_url"))
            # Use .get with a default empty list to safely join keywords or tickers
            keywords_list.append(','.join(article.get("keywords", [])))
            tickers_list.append(','.join(article.get("tickers", [])))
            titles.append(article.get("title"))

            # For publisher data, use an empty dict as fallback
            publisher = article.get("publisher") or {}
            favicons.append(publisher.get("favicon_url"))
            names.append(publisher.get("name"))
            # Assuming logo_url is the same as favicon_url per your original code
            logo_urls.append(publisher.get("favicon_url"))
            homepage_urls.append(publisher.get("homepage_url"))

        # Assign to instance attributes (keeping dot notation)
        self.amp_url = amp_urls
        self.article_url = article_urls
        self.author = authors
        self.description = descriptions
        self.id = ids
        self.image_url = image_urls
        self.keywords = keywords_list
        self.tickers = tickers_list
        self.title = titles
        self.favicon_url = favicons
        self.name = names
        self.logo_url = logo_urls
        self.homepage_url = homepage_urls

        # Build a data dictionary and a corresponding DataFrame for convenience
        self.data_dict = {
            'title': self.title,
            'name': self.name,
            'author': self.author,
            'article_url': self.article_url,
            'article_url': self.homepage_url,
            'logo_url': self.logo_url,
            'image_url': self.image_url,
            'ticker_mentioned': self.tickers,
            'description': self.description,
            'keywords': self.keywords
        }
        self.as_dataframe = pd.DataFrame(self.data_dict)
