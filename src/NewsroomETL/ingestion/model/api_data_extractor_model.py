import requests
import logging
from includes.utils import SparkUtils
from includes.utils import get_env_conf
from datetime import datetime

logger = logging.getLogger(__name__)
config = get_env_conf()


class APIDataExtractor(object):
    def __init__(self, category_post: str, url: str, source: str) -> None:
        self.url = url
        self.category = category_post
        self.bucket_folder = source
        self.bucket_name = config["s3"]["bucket_zones"]["bronze_zone"]
        self.environment = config["environment"]
        self.file_path = self.get_file_path()
        self.bucket_and_file_path = f"{self.bucket_name}/{self.file_path}"
        self.page_size = 1

    def get_file_path(self) -> str:
        current_date = datetime.now().strftime("%Y-%m-%d")
        filename = f"{self.category}_{current_date}.json"
        category_path = f"{self.category}_articles"
        return f"{self.environment}/{self.bucket_folder}/{category_path}/{filename}"

    def extract_and_store_data(self, page: int = 1) -> None:
        params = self.get_query_params(page)

        try:
            logging.info(f"Attempting to extract data from {self.url}/{self.category}")
            response = requests.get(self.url, params=params)
            response.raise_for_status()
            SparkUtils().write_s3_json(
                data=response.json(), bucket=self.bucket_name, file_path=self.file_path
            )
        except requests.exceptions.RequestException as err:
            logging.error(f"The request had invalid params: {err}")
