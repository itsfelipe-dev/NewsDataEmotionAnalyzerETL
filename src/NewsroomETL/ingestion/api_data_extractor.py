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

    def get_query_params(self) -> dict:
        return {
            "fl": "id,web_url,headline,abstract,pub_date,body,section_name,byline,keywords,subsection_name,source,desk,multimedia,type_of_material",
            "fq": f"section_name={self.section}",
            "sort": "newest",
            "page": self.offset,  #! implement concurrent
            "api-key": self.api_key,
        }


class TheGuardian(APIDataExtractor):
    def __init__(self, category_post: str) -> None:
        self.source = config["data_sources"]["theguardian"]["name"]
        self.api_key = config["data_sources"]["theguardian"]["api_key"]
        self.url = "https://content.guardianapis.com/search?"
        super().__init__(category_post, self.url, self.source)

    def get_query_params(self) -> dict:
        return {
            "show-fields": "byline,headline,standfirst,trailText,body,publication,productionOffice,wordcount,lastModified",
            "show-tags": "tone,type,keyword,contributor",
            "show-section": "true",
            "show-elements": "audio,image",
            "lang": "en",
            "order-by": "newest",
            "page-size": self.page_size,
            "page": self.offset,  #! implement concurrent
            "api-key": self.api_key,
            "section": self.section,
        }
