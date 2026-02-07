import findspark
findspark.init()
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from botocore.exceptions import ClientError
import json
import logging
import boto3
import yaml

logger = logging.getLogger(__name__)


class SparkUtils(object):
    def __init__(self) -> None:
        pass

    def get_spark_session(self, app_name) -> SparkSession:
        conf = SparkConf()
        conf.setAll(
            [
                ("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2"),
                (
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.profile.ProfileCredentialsProvider",
                ),
            ]
        )
        session = (
            SparkSession.builder.master("local[*]")
            .appName(app_name)
            .config(conf=conf)
            .getOrCreate()
        )
        return session

    def read_s3_json(self, file_path: str, spark: SparkSession) -> DataFrame:
        logging.info(f"Attempting to read data from: {file_path}")
        try:
            return spark.read.option("inferSchema", "true").json(
                f"s3a://{file_path}",
                encoding="utf8",
            )
        except ClientError as e:
            logging.error(f"Error writing data in S3: {e}")
            return None

    def write_s3_json(self, data: list, bucket: str, file_path: str) -> None:
        s3_client = boto3.client("s3")
        logging.info(f"Attempting to write data in: {bucket}/{file_path}")
        try:
            s3_client.put_object(Body=json.dumps(data), Bucket=bucket, Key=file_path)
        except ClientError as e:
            logging.error(f"Error writing data in S3: {e}")

    def write_s3_parquet(
        self,
        df: DataFrame,
        file_path: str,
        mode="overwrite",
        partition="ingestion_date",
    ):
        logging.info(f"Attempting to write data in: {file_path}")
        try:
            df.write.mode(mode).partitionBy(partition).parquet(f"s3a://{file_path}")
        except ClientError as e:

    def read_s3_parquet(self, file_path: str, spark: SparkSession) -> DataFrame:
        logging.info(f"Attempting to read parquet from: {file_path}")
        try:
            return spark.read.parquet(f"s3a://{file_path}")
        except Exception as e:
            logging.error(f"Error reading parquet in S3: {e}, {file_path}")
            raise


def get_env_conf() -> list:
    with open("./includes/config.yml", "r") as f:
        return yaml.safe_load(f)
