from includes.utils import get_env_conf
from trans.adapters.theguardian import TheGuardianAdapter
from trans.adapters.nytimes import NyTimesAdapter
from trans.model.base_processor import BaseProcessor
from trans.model.silver_models import SilverModels
from writers.delta_writer import DeltaWriter
from trans.silver_processor import SilverProcessor
from readers.json_reader import JsonReader
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

config = get_env_conf()


class SilverOrchestrator:
    ADAPTERS = {"nytimes": NyTimesAdapter, "theguardian": TheGuardianAdapter}

    def __init__(self, spark):
        self.spark = spark

    def run(self, source, date):
        writer = DeltaWriter()
        reader = JsonReader()
        silver_processor = SilverProcessor()
        base = BaseProcessor()
        sections = config["data_sources"][source]["sections"]
        adapter = self.ADAPTERS[source](source)

        for section in sections:
            bronze_df = reader.read(
                file_path=base.get_bronze_path(section, date, source),
                spark=self.spark,
            )

            if bronze_df.head(1):
                pre_silver_dfs = adapter.process(bronze_df, "article_media")
                if not pre_silver_dfs:
                    logger.warning(f"Pre-silver has no tables for {source} - {section}")
                    return

                for table_name, df in pre_silver_dfs.items():
                    if not df.head(1):
                        logger.warning(
                            f"Table {table_name} is empty for {source} - {section}"
                        )
                    self._process_table(
                        table_name=table_name,
                        df=df,
                        silver_processor=silver_processor,
                        base=base,
                        writer=writer,
                        source=source,
                    )
            else:
                logger.warning(f"No data found for {source} - {section}")

    def _process_table(self, table_name, df, silver_processor, base, writer, source):
        table_config = SilverModels.TABLE_CONFIGS.get(table_name, {})
        pk_col = table_config.get("pk_name")
        partition_cols = table_config.get("partition_by")

        silver_df = silver_processor.standardize(df, table_name)
        silver_path = base.get_silver_path(table_name)
        try:
            writer.write(
                df=silver_df,
                file_path=silver_path,
                pk_col=pk_col,
                partition_cols=partition_cols,
                spark=self.spark,
                force_overwrite=True
            )
            logger.info(f"Successfully stored data in: {silver_path}")

        except Exception as e:
            logger.error(f"Can't store data in: silver_path - {e}")
