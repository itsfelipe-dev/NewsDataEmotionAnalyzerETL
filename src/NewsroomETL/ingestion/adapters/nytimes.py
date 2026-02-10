from ..model.api_data_extractor_model import APIDataExtractor
from includes.utils import get_env_conf

config = get_env_conf()


class NyTimes(APIDataExtractor):
    def __init__(
        self,
        category_post: str,
        page_size: int,
    ) -> None:
        self.source = config["data_sources"]["nytimes"]["name"]
        self.api_key = config["data_sources"]["nytimes"]["api_key"]
        self.url = "https://api.nytimes.com/svc/search/v2/articlesearch.json"
        self.page_size = page_size
        self.section = category_post
        super().__init__(self.url, self.source)

    def get_query_params(self, offset) -> dict:
        return {
            "fl": "id,web_url,headline,abstract,pub_date,body,section_name,byline,keywords,subsection_name,source,desk,multimedia,type_of_material",
            "fq": f"section_name={self.section}",
            "sort": "newest",
            "page": offset, 
            "api-key": self.api_key,
        }
