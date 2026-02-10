from ..model.api_data_extractor_model import APIDataExtractor
from includes.utils import get_env_conf

config = get_env_conf()


class TheGuardian(APIDataExtractor):
    def __init__(
        self,
        category_post: str,
        page_size: int,
    ) -> None:
        self.source = config["data_sources"]["theguardian"]["name"]
        self.api_key = config["data_sources"]["theguardian"]["api_key"]
        self.url = "https://content.guardianapis.com/search?"
        self.page_size = page_size
        self.section = category_post
        super().__init__(self.url, self.source)

    def get_query_params(self, offset: int) -> dict:
        return {
            "show-fields": "byline,headline,standfirst,trailText,body,publication,productionOffice,wordcount,lastModified",
            "show-tags": "tone,type,keyword,contributor",
            "show-section": "true",
            "show-elements": "audio,image",
            "lang": "en",
            "order-by": "newest",
            "page-size": self.page_size,
            "page": offset, 
            "api-key": self.api_key,
            "section": self.section,
        }
