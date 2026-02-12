from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DateType,
    BooleanType,
)


class SilverModels:
    SOURCES_SCHEMA = StructType(
        [
            StructField("source_system", StringType(), False),
            StructField("source_name", StringType(), True),
            StructField("api_url", StringType(), True),
            StructField("ingestion_date", DateType(), True),
        ]
    )

    SECTIONS_SCHEMA = StructType(
        [
            StructField("section_id", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("source_section_id", StringType(), True),
            StructField("section_name", StringType(), True),
            StructField("subsection_name", StringType(), True),
            StructField("ingestion_date", DateType(), True),
        ]
    )

    ARTICLES_SCHEMA = StructType(
        [
            StructField("article_id", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("source_article_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("headline", StringType(), True),
            StructField("publication_date", TimestampType(), True),
            StructField("section_id", StringType(), True),
            StructField("is_live", BooleanType(), True),
            StructField("wordcount", IntegerType(), True),
            StructField("ingestion_date", DateType(), True),
        ]
    )

    ARTICLE_CONTENT_SCHEMA = StructType(
        [
            StructField("article_id", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("body_text", StringType(), True),
            StructField("summary_text", StringType(), True),
            StructField("trail_text", StringType(), True),
            StructField("last_modified", TimestampType(), True),
            StructField("ingestion_date", DateType(), True),
        ]
    )

    AUTHORS_SCHEMA = StructType(
        [
            StructField("author_id", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("source_author_id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("bio", StringType(), True),
            StructField("twitter_handle", StringType(), True),
            StructField("ingestion_date", DateType(), True),
        ]
    )

    ARTICLE_MEDIA_SCHEMA = StructType(
        [
            StructField("media_id", StringType(), False),
            StructField("article_id", StringType(), True),
            StructField("source_system", StringType(), False),
            StructField("media_type", StringType(), True),
            StructField("media_url", StringType(), True),
            StructField("mime_type", StringType(), True),
            StructField("caption", StringType(), True),
            StructField("credit", StringType(), True),
            StructField("position", IntegerType(), True),
            StructField("is_primary", BooleanType(), True),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True),
            StructField("relation", StringType(), True),
            StructField("duration_seconds", IntegerType(), True),
            StructField("thumbnail_url", StringType(), True),
            StructField("ingestion_date", DateType(), True),
        ]
    )

    TAGS_SCHEMA = StructType(
        [
            StructField("tag_id", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("tag_type", StringType(), True),
            StructField("tag_value", StringType(), True),
            StructField("ingestion_date", DateType(), True),
        ]
    )

    # Bridge Tables
    ARTICLE_AUTHORS_SCHEMA = StructType(
        [
            StructField("article_id", StringType(), False),
            StructField("author_id", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("ingestion_date", DateType(), True),
        ]
    )

    ARTICLE_TAGS_SCHEMA = StructType(
        [
            StructField("article_id", StringType(), False),
            StructField("tag_id", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("ingestion_date", DateType(), True),
        ]
    )

    TABLE_CONFIGS = {
        "sources": {"schema": SOURCES_SCHEMA, "business_keys": ["source_system"]},
        "sections": {
            "schema": SECTIONS_SCHEMA,
            "business_keys": ["source_system", "source_section_id"],
            "pk_name": "section_id",
        },
        "articles": {
            "schema": ARTICLES_SCHEMA,
            "business_keys": ["source_system", "source_article_id"],
            "pk_name": "article_id",
            "partition_by": ["ingestion_date"],
        },
        "article_content": {
            "schema": ARTICLE_CONTENT_SCHEMA,
            "business_keys": ["source_system", "source_article_id"],
            "pk_name": "article_id",
            "partition_by": ["ingestion_date"],
        },
        "authors": {
            "schema": AUTHORS_SCHEMA,
            "business_keys": ["source_system", "source_author_id"],
            "pk_name": "author_id",
        },
        "article_media": {
            "schema": ARTICLE_MEDIA_SCHEMA,
            "business_keys": ["source_system", "media_url"],
            "pk_name": "media_id",
        },
        "tags": {
            "schema": TAGS_SCHEMA,
            "business_keys": ["source_system", "tag_type", "tag_value"],
            "pk_name": "tag_id",
        },
        "article_authors": {
            "schema": ARTICLE_AUTHORS_SCHEMA,
            "business_keys": ["article_id", "author_id"],
            "partition_by": ["ingestion_date"],
        },
        "article_tags": {
            "schema": ARTICLE_TAGS_SCHEMA,
            "business_keys": ["article_id", "tag_id"],
            "partition_by": ["ingestion_date"],
        },
    }
