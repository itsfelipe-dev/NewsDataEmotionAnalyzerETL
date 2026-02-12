from pyspark.sql.functions import (
    md5,
    concat_ws,
    col,
    current_timestamp,
    lit,
    to_date,
    lower,
    trim,
)
from trans.model.silver_models import SilverModels
from pyspark.sql.functions import udf
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType
from bs4 import BeautifulSoup
import re
import logging

logger = logging.getLogger(__name__)


class SilverProcessor:
    @staticmethod
    def expr_surrogate_key(col_names: list) -> str:
        # Generates the MD5 String ID
        return md5(concat_ws("||", *[col(c) for c in col_names]))

    @staticmethod
    @udf(returnType=StringType())
    def clean_html(raw_html: str) -> str:
        if not raw_html:
            return ""
        soup = BeautifulSoup(raw_html, "html.parser")
        text = soup.get_text(separator=" ")

        text = re.sub(r"\s+", " ", text).strip()

        return text

    def standardize(self, df: DataFrame, table_name: str) -> DataFrame:
        logger.info(f"Standardizing table: {table_name}")

        config = SilverModels.TABLE_CONFIGS.get(table_name)
        if not config:
            logger.error(f"Configuration not found for table: {table_name}")
            raise ValueError(f"No silver model defined for table: {table_name}")

        df = self._normalize_values(table_name=table_name, df=df)
        b_keys = config.get("business_keys")
        pk_key = config.get("pk_name")

        if pk_key:
            df = df.withColumn(pk_key, self.expr_surrogate_key(b_keys))

        df = self._apply_foreign_keys(df=df, table_name=table_name)
        df = self._align_schema(table_name=table_name, df=df, config=config)
        df = self._add_metadata(df=df)

        # Drop duplicates
        if pk_key:
            df = df.dropDuplicates([pk_key])

        logger.info(f"Standardization finalized for {table_name}")
        return df

    def _apply_foreign_keys(
        self,
        df: DataFrame,
        table_name: str,
    ) -> DataFrame:
        if table_name == "articles":
            df = df.withColumn(
                "section_id",
                self.expr_surrogate_key(["source_system", "source_section_id"]),
            )

        elif table_name == "article_media":
            df = df.withColumn(
                "article_id",
                self.expr_surrogate_key(["source_system", "source_article_id"]),
            )

        elif table_name == "article_authors":
            df = df.withColumn(
                "article_id",
                self.expr_surrogate_key(["source_system", "source_article_id"]),
            )
            df = df.withColumn(
                "author_id",
                self.expr_surrogate_key(["source_system", "source_author_id"]),
            )

        elif table_name == "article_tags":
            df = df.withColumn(
                "article_id",
                self.expr_surrogate_key(["source_system", "source_article_id"]),
            )
            df = df.withColumn(
                "tag_id",
                self.expr_surrogate_key(["source_system", "tag_type", "tag_value"]),
            )
        return df

    def _align_schema(self, df: DataFrame, config: dict, table_name: str) -> DataFrame:
        schema = config.get("schema")
        # Ensure all columns in the schema exist
        target_columns = [field.name for field in schema]
        current_columns = df.columns

        missing_in_source = [c for c in target_columns if c not in current_columns]
        if missing_in_source:
            logger.warning(
                f"Columns {missing_in_source} missing in {table_name} default null"
            )
            for col_name in missing_in_source:
                df = df.withColumn(col_name, lit(None))

        # Force types
        df = df.select(*[col(f.name).cast(f.dataType) for f in schema])

        return df

    def _add_metadata(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("processed_at", to_date(current_timestamp()))
        return df

    def _normalize_values(self, df: DataFrame, table_name: str) -> DataFrame:
        if table_name == "article_content":
            df = df.withColumn("body_text", self.clean_html(df["body_text"]))
            df = df.withColumn("trail_text", self.clean_html(df["trail_text"]))
            df = df.withColumn("summary_text", self.clean_html(df["summary_text"]))
            return df

        if table_name == "authors":
            cols_to_normalize = ["first_name", "last_name", "twitter_handle"]
            for c in cols_to_normalize:
                df = df.withColumn(c, lower(trim(col(c))))
            df = df.withColumn("bio", self.clean_html(df["bio"]))

        if table_name == "tags":
            df = df.withColumn("tag_value", lower(trim(col("tag_value"))))

        return df
