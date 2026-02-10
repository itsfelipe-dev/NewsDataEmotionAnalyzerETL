import logging
from includes.utils import SparkUtils
from includes.utils import get_env_conf

logger = logging.getLogger(__name__)
config = get_env_conf()


class BaseProcessor(object):
    def __init__(self,source) -> None:
        self.bucket_output = config["s3"]["bucket_zones"]["silver_zone"]
        self.bucket_input = config["s3"]["bucket_zones"]["bronze_zone"]
        self.environment = config["environment"]
        self.spark_utils = SparkUtils()
        self.source = source
        self.spark = self.spark_utils.get_spark_session(app_name=f"DataTransformation")

    def get_bronze_path(self, section, ingestion_date) -> str:
        return f"{self.bucket_input}/{self.environment}/{self.source}/ingestion_date={ingestion_date}/section={section}/"

    def get_silver_path(self, table_name) -> str:
        return f"{self.bucket_output}/{self.environment}/{table_name}"