import findspark
findspark.init()
from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkUtils(object):
    def __init__(self):
        self.session = None

    def get_spark_session(self, app_name) -> SparkSession:
        if self.session:
            return self.session

        conf = SparkConf()
        conf.setAll(
            [
                (
                    "spark.jars.packages",
                    "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.2.2",
                ),
                (
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.profile.ProfileCredentialsProvider",
                ),
                (
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                ),
                (
                    "spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension",
                ),
            ]
        )
        self.session = (
            SparkSession.builder.master("local[*]")
            .appName(app_name)
            .config(conf=conf)
            .getOrCreate()
        )
        return self.session

    def stop_spark_session(self) -> None:
        if self.session:
            self.session.stop()
            self.session = None


def get_env_conf() -> list:
    with open("./includes/config.yml", "r") as f:
        return yaml.safe_load(f)
