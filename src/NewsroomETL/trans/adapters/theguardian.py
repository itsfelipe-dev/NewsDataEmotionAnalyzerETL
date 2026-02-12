import logging
from includes.utils import SparkUtils
from pyspark.sql.functions import col, explode, lit
from pyspark.sql.dataframe import DataFrame

logger = logging.getLogger(__name__)


spark_utils = SparkUtils()


class TheGuardianAdapter:
    def __init__(self, source_name: str):
        self.source_name = source_name

    def extract_articles(self, df_raw: DataFrame) -> DataFrame:
        return df_raw.select(
            col("article.id").alias("source_article_id"),
            col("source_system"),
            col("article.apiUrl").alias("url"),
            col("article.sectionId").alias("source_section_id"),
            col("article.fields.headline").alias("headline"),
            col("article.webPublicationDate").alias("publication_date"),
            col("article.sectionId").alias("section_id"),
            lit(True).alias("is_live"),
            col("article.fields.wordcount").alias("wordcount"),
            col("ingestion_date"),
        )

    def extract_sections(self, df_raw: DataFrame) -> DataFrame:
        return df_raw.select(
            col("source_system"),
            col("article.sectionId").alias("source_section_id"),
            col("article.sectionName").alias("section_name"),
            lit(None).alias("subsection_name"),
            col("ingestion_date"),
        )

    def extract_authors(self, df_raw: DataFrame) -> DataFrame:
        df_tags = df_raw.select(
            col("ingestion_date"),
            col("source_system"),
            explode("article.tags").alias("tags"),
        ).filter(col("tags.type") == "contributor")

        if "twitterHandle" not in df_tags.select("tags.*").columns:
            df_tags = df_tags.withColumn("twitter_handle", lit(None))
        else:
            df_tags = df_tags.withColumn("twitter_handle", col("tags.twitterHandle"))

        return df_tags.select(
            col("source_system"),
            col("tags.id").alias("source_author_id"),
            col("tags.firstName").alias("first_name"),
            col("tags.lastName").alias("last_name"),
            col("tags.bio").alias("bio"),
            col("twitter_handle"),
            col("ingestion_date"),
        )

    def extract_tags(self, df_raw: DataFrame) -> DataFrame:
        return df_raw.select(
            col("ingestion_date"),
            col("source_system"),
            explode("article.tags").alias("tags"),
        ).select(
            col("source_system"),
            col("tags.type").alias("tag_type"),
            col("tags.webTitle").alias("tag_value"),
            col("ingestion_date"),
        )

    def extract_sources(self, df_raw: DataFrame) -> DataFrame:
        return df_raw.select(
            col("source_system"),
            lit("The Guardian").alias("source_name"),
            col("article.apiUrl").alias("api_url"),
            col("ingestion_date"),
        )

    def extract_media(self, df_raw: DataFrame) -> DataFrame:
        return (
            df_raw.select(
                col("source_system"),
                col("ingestion_date"),
                col("article.id").alias("source_article_id"),
                explode("article.elements").alias("elements"),
            )
            .select(
                col("source_system"),
                col("ingestion_date"),
                col("source_article_id"),
                col("elements.relation").alias("relation"),
                explode("elements.assets").alias("assets"),
            )
            .select(
                col("source_system"),
                col("source_article_id"),
                col("assets.type").alias("media_type"),
                col("assets.file").alias("media_url"),
                col("assets.mimeType").alias("mime_type"),
                col("assets.typeData.caption").alias("caption"),
                col("assets.typeData.credit").alias("credit"),
                lit(None).alias("position"),
                lit(None).alias("is_primary"),
                col("assets.typeData.width").alias("width"),
                col("assets.typeData.height").alias("height"),
                col("relation"),
                lit(None).alias("duration_seconds"),
                lit(None).alias("thumbnail_url"),
                col("ingestion_date"),
            )
        )

    def extract_content(self, df_raw: DataFrame) -> DataFrame:
        return df_raw.select(
            col("article.id").alias("source_article_id"),
            col("source_system"),
            col("article.fields.body").alias("body_text"),
            col("article.fields.standfirst").alias("summary_text"),
            col("article.fields.trailText").alias("trail_text"),
            col("article.fields.lastModified").alias("last_modfied"),
            col("ingestion_date"),
        )

    def extract_article_authors(self, df_raw: DataFrame) -> DataFrame:
        return (
            df_raw.select(
                col("source_system"),
                col("ingestion_date"),
                col("article.id").alias("source_article_id"),
                explode("article.tags").alias("tag"),
            )
            .filter(col("tag.type") == "contributor")
            .select(
                col("source_system"),
                col("source_article_id"),
                col("ingestion_date"),
                col("tag.id").alias("source_author_id"),
            )
        )

    def extract_article_tags(self, df_raw: DataFrame) -> DataFrame:
        return (
            df_raw.select(
                col("source_system"),
                col("ingestion_date"),
                col("article.id").alias("source_article_id"),
                explode("article.tags").alias("tag"),
            )
            .filter(col("tag.type") != "contributor")
            .select(
                col("source_system"),
                col("source_article_id"),
                col("ingestion_date"),
                col("tag.type").alias("tag_type"),
                col("tag.webTitle").alias("tag_value"),
            )
        )

    def process(self, bronze_df, table_to_process=None):
        try:
            base_df = bronze_df.select(
                col("source_system"),
                col("ingested_at").alias("ingestion_date"),
                explode("response_payload.response.results").alias("article"),
            ).cache()

            extractions = {
                "articles": self.extract_articles,
                "authors": self.extract_authors,
                "tags": self.extract_tags,
                "article_media": self.extract_media,
                "sections": self.extract_sections,
                "sources": self.extract_sources,
                "article_content": self.extract_content,
                "article_authors": self.extract_article_authors,
                "article_tags": self.extract_article_tags,
            }

            if table_to_process:
                targets = {table_to_process: extractions[table_to_process]}
            else:
                targets = extractions

            results = {}

            for table_name, extract_func in targets.items():
                logger.info(f"Parsing schema for table: {table_name}...")
                results[table_name] = extract_func(base_df)
                cols = results[table_name].columns
                logger.info(f"Schema validated for {table_name}. Columns: {cols}")

            return results

        except Exception as e:
            logger.error(f"Pipeline failed during extraction phase {e}")
            raise RuntimeError("TheGuardianAdapter.process failed") from e
