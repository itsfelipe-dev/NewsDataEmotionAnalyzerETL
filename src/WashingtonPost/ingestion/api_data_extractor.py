from .model.api_data_extractor_model import APIDataExtractor
from includes.utils import get_env_conf


config = get_env_conf()


class WashingtonPost(APIDataExtractor):
    def __init__(self, category_post: str) -> None:
        self.source = config["data_sources"]["wp"]["name"]
        self.url = "https://www.washingtonpost.com/prism/api/prism-query"
        super().__init__(category_post, self.url, self.source)

    def get_query_params(self, offset: int) -> dict:
        #! implement concurrent
        return {
            "_website": "washpost",
            "query": f'{{"query":"prism://prism.query/site-articles-only,/{self.category}&limit={self.page_size}&offset={offset}"}}',
            "filter": "{items{_id display_date headlines publish_date taxonomy{tags}}}",
        }


class NyTimes(APIDataExtractor):
    def __init__(self, category_post: str) -> None:
        self.source = config["data_sources"]["nytimes"]["name"]
        self.api_key = config["data_sources"]["nytimes"]["api_key"]
        self.url = "https://api.nytimes.com/svc/search/v2/articlesearch.json"
        super().__init__(category_post, self.url, self.source)

    def get_query_params(self, page: int) -> dict:
        return {
            "fl": "id,web_url,headline,abstract,pub_date,body,section_name,byline,keywords,subsection_name,source",
            "fq": f"section_name={self.category}",
            "sort": "newest",
            "page": page,  #! implement concurrent
            "api-key": self.api_key,
        }


class TheGuardian(APIDataExtractor):
    def __init__(self, category_post: str) -> None:
        self.source = config["data_sources"]["theguardian"]["name"]
        self.api_key = config["data_sources"]["theguardian"]["api_key"]
        self.url = "https://content.guardianapis.com/search?"
        pass
        super().__init__(category_post, self.url, self.source)

    def get_query_params(self, offset: int) -> dict:
        if not offset:
            offset = 1
        return {
            "fl": "id,web_url,headline,abstract,pub_date,body,section_name,byline,keywords,subsection_name,source",
            "q": f"NOT= 20cartoon",
            "order-by": "newest",
            "page-size": self.page_size,
            "page": offset,  #! implement concurrent
            "api-key": self.api_key,
            "section": self.category,
        }
