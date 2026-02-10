from ..model.api_data_extractor_model import APIDataExtractor
from includes.utils import get_env_conf

config = get_env_conf()


class WashingtonPost(APIDataExtractor):
    def __init__(self, category_post: str) -> None:
        self.source = config["data_sources"]["wp"]["name"]
        self.url = "https://www.washingtonpost.com/prism/api/prism-query"
        self.section = category_post
        super().__init__(self.url, self.source)

    def get_query_params(self, offset: int) -> dict:
        return {
            "_website": "washpost",
            "query": f'{{"query":"prism://prism.query/site-articles-only,/{self.category}&limit={self.page_size}&offset={offset}"}}',
            "filter": "{items{_id display_date headlines publish_date taxonomy{tags}}}",
        }
