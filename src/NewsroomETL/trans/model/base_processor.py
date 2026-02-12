import logging
from includes.utils import SparkUtils
from includes.utils import get_env_conf

logger = logging.getLogger(__name__)
config = get_env_conf()


class BaseProcessor(object):
    def __init__(self) -> None:
        self.bucket_bronze = config["s3"]["bucket_zones"]["bronze_zone"]
        self.bucket_silver = config["s3"]["bucket_zones"]["silver_zone"]
        self.bucket_gold = config["s3"]["bucket_zones"]["gold_zone"]
        self.environment = config["environment"]

    def get_bronze_path(self, section, ingestion_date, source) -> str:
        return f"{self.bucket_bronze}/{self.environment}/{source}/ingestion_date={ingestion_date}/section={section}/"

    def get_silver_path(self, table_name) -> str:
        return f"{self.bucket_silver}/{self.environment}/{table_name}"

    def get_gold_path(self, table_name) -> str:
        return f"{self.bucket_gold}/{self.environment}/{table_name}"
