import logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

logger = logging.getLogger(__name__)


class JsonReader:
    def read(self, file_path: str, spark: SparkSession) -> DataFrame:
        logger.info(f"Attempting to read JSON from: {file_path}")
        try:
            return spark.read.option("inferSchema", "true").json(
                f"s3a://{file_path}",
                encoding="utf8",
            )
        except Exception as e:
            logger.error(f"Error reading JSON in S3: {e}")
            raise
