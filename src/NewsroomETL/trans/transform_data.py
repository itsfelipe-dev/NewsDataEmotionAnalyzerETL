import logging
import yaml
import os
from includes.utils import SparkUtils
from pyspark.sql.functions import col, explode, current_date, lit
from warnings import warn
from includes.utils import get_env_conf

logger = logging.getLogger(__name__)





config = get_env_conf()
spark_utils = SparkUtils()


class TransformData(object):
    def __init__(self) -> None:
        self.bucket_name = config["s3"]["bucket_zones"]["silver_zone"]
        self.source = config["data_sources"]["wp"]["name"]
        self.destination = config["s3"]["destination"]["silver"]
        self.environment = config["environment"]

    def get_category_file(self, file_path):
        return os.path.basename(file_path).split("_")[0]


    def trans_wp_data(self, file_path: str):
        warn('This method is deprecated because the data is filter in source side instead.', DeprecationWarning, stacklevel=2)
        category_file = self.get_category_file(file_path)
        spark = spark_utils.get_spark_session(app_name=f"{self.source}_trans")
        logging.info(f"Transforming data from {file_path}")
        df_original = spark_utils.read_s3_json(file_path, spark)
        df_explode_stage_1 = df_original.select("_id", explode("items").alias("items"))
        df_explode_stage_2 = df_explode_stage_1.select("items.*")
        final_df = df_explode_stage_2.select(
            col("headlines.basic").alias("headline"),
            col("canonical_url").alias("post_url"),
            col("additional_properties.publish_date"),
            col("additional_properties.time_to_read"),
            lit(self.source).alias("publication_source"),
            lit(category_file).alias("category"),
            col("label.basic.text").alias("label"),
            col("credits.by._id").alias("authors_id"),
            col("credits.by.name").alias("authors_name"),
            col("credits.by.url").alias("authors_url"),
            lit(current_date()).alias("ingestion_date"),
        )

        spark_utils.write_s3_parquet(
            df=final_df,
            file_path=f"{self.bucket_name}/{self.environment}/{self.destination}",
            mode="append",
        )
