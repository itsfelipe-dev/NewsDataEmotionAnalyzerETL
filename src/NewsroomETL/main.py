from datetime import datetime
from includes.utils import get_env_conf
from ingestion.bronze_orchestrator import BronzeOrchestaror
from trans.silver_orchestrator import SilverOrchestrator
from includes.utils import SparkUtils
import logging

logger = logging.getLogger(__name__)


def main() -> None:
    config = get_env_conf()
    sources = config["data_sources"]

    spark_utils = SparkUtils()
    spark = spark_utils.get_spark_session(app_name=f"SilverDataTransformation")

    bronze_orch = BronzeOrchestaror()
    silver_orch = SilverOrchestrator(spark)
    
    run_date = datetime.now().strftime("%Y-%m-%d")
    for source in sources:
        try:
            logger.info(f"Starting pipeline for source: {source}")
            bronze_orch.run(source)
            silver_orch.run(source, run_date)
            logger.info(f"Successfully processed: {source}")
        except Exception as e:
            logger.error(f"Failed to process source {source}: {e}")
            continue

    spark_utils.stop_spark_session()


if __name__ == "__main__":
    main()
