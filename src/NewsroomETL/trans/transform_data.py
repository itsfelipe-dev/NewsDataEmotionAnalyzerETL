import logging
import yaml
import os
from includes.utils import SparkUtils
from pyspark.sql.functions import col, explode, current_date, lit, to_json, when
from pyspark.sql.dataframe import DataFrame
from warnings import warn
from includes.utils import get_env_conf

from .model.transform_data_model import DataTransformer

logger = logging.getLogger(__name__)


config = get_env_conf()
spark_utils = SparkUtils()


class TheGuardian(DataTransformer):
    def __init__(self) -> None:
        self.source = config["data_sources"]["theguardian"]["name"]
        super().__init__()

    def tranform_data(self, ingestion_date: str, category_post: str) -> None:
        try:
            df = self.get_bronze_df(
                category_post=category_post,
                ingestion_date=ingestion_date,
                source=self.source,
            )
            df_transformed = (
                df.select(explode("response.results"))
                .select("col.*")
                .select(
                    col("fields.bodyText").alias("body"),
                    col("fields.byline").alias("author"),
                    col("fields.trailText").alias("subheading"),
                    col("sectionId").alias("section_internal_name"),
                    lit(category_post).alias("section_query_name"),
                    col("webTitle").alias("headline"),
                    col("webUrl").alias("web_url"),
                    col("webPublicationDate").alias("pub_date"),
                    lit(self.source).alias("source"),
                    lit(ingestion_date).alias("ingestion_date"),
                )
            )
            self.load_silver_df(self.standardize_schema(df_transformed))
            logger.info(
                f"Transformation completed for {self.source}/{category_post} on {ingestion_date}"
            )
        except Exception as e:
            logger.error(
                f"Error transforming data for {category_post} on {ingestion_date}: {str(e)}"
            )


class NyTimes(DataTransformer):
    def __init__(self) -> None:
        self.source = config["data_sources"]["nytimes"]["name"]
        super().__init__()

    def tranform_data(self, ingestion_date: str, category_post: str) -> None:
        try:
            df = self.get_bronze_df(
                category_post=category_post,
                ingestion_date=ingestion_date,
                source=self.source,
            )
            df_transformed = (
                df.select(explode("response.docs"))
                .select("col.*")
                .select(
                    col("abstract").alias("subheading"),
                    col("byline.original").alias("author"),
                    col("headline.main").alias("headline"),
                    to_json(col("keywords")).alias("keywords"),
                    col("section_name").alias("section_internal_name"),
                    lit(category_post).alias("section_query_name"),
                    when(col("subsection_name").isNotNull(), col("subsection_name")).otherwise(lit(None)).alias("subsection_name"),
                    col("web_url"),
                    col("pub_date"),
                    lit(self.source).alias("source"),
                    lit(ingestion_date).alias("ingestion_date"),
                )
            )
            self.load_silver_df(self.standardize_schema(df_transformed))
            logger.info(
                f"Transformation completed for {self.source}/{category_post} on {ingestion_date}"
            )
        except Exception as e:
            logger.error(
                f"Error transforming data for {category_post} on {ingestion_date}: {str(e)}"
            )
