import logging
from includes.utils import SparkUtils
from includes.utils import get_env_conf
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode, current_date, lit
from pyspark.sql.types import StringType, DateType, TimestampType, ArrayType

logger = logging.getLogger(__name__)
config = get_env_conf()


class DataTransformer(object):
    def __init__(self) -> None:
        self.bucket_output = config["s3"]["bucket_zones"]["silver_zone"]
        self.bucket_input = config["s3"]["bucket_zones"]["bronze_zone"]
        self.silver_zone_table = "unified_news"
        self.environment = config["environment"]
        self.spark_utils = SparkUtils()
        self.spark = self.spark_utils.get_spark_session(app_name=f"DataTransformation")

    def get_path_input_df(self, category_post, ingestion_date, source) -> str:
        filename = f"{category_post}_{ingestion_date}.json"
        category_path = f"{category_post}_articles"
        return f"{self.bucket_input}/{self.environment}/{source}/{category_path}/{filename}"

    def get_path_output_df(self) -> str:
        return f"{self.bucket_output}/{self.environment}/{self.silver_zone_table}"

    def get_bronze_df(self, category_post, ingestion_date, source) -> DataFrame:
        file_path = self.get_path_input_df(category_post, ingestion_date, source)
        return self.spark_utils.read_s3_json(file_path, self.spark)

    def load_silver_df(self, df: DataFrame):
        path_output_df = self.get_path_output_df()
        self.spark_utils.write_s3_parquet(
            df=df, file_path=path_output_df, mode="append"
        )

    def standardize_schema(self, df: DataFrame) -> DataFrame:
        return df.select(
            col("subheading").cast(StringType()),
            col("author").cast(StringType()),
            col("headline").cast(StringType()),
            (
                col("body").cast(StringType())
                if "body" in df.columns
                else lit(None).cast(StringType()).alias("body")
            ),
            (
                col("keywords").cast(StringType())
                if "keywords" in df.columns
                else lit(None).cast(StringType()).alias("keywords")
            ),
            col("pub_date").cast(TimestampType()),
            col("section_internal_name").cast(StringType()),
            col("section_query_name").cast(StringType()),
            (
                col("subsection_name").cast(StringType())
                if "subsection_name" in df.columns
                else lit(None).cast(StringType()).alias("subsection_name")
            ),
            col("source").cast(StringType()),
            col("web_url").cast(StringType()),
            col("ingestion_date").cast(DateType()),
        )
