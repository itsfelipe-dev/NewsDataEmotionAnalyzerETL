from pyspark.sql import DataFrame
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)


class DeltaWriter:
    def write(
        self,
        spark,
        df: DataFrame,
        file_path: str,
        pk_col: str = None,
        partition_cols: list = None,
        force_overwrite: bool = False,
    ) -> None:
        full_path = f"s3a://{file_path}"

        # Overwrite
        if not DeltaTable.isDeltaTable(spark, full_path) or force_overwrite == True:
            logger.info(f"Table {file_path} does not exist. Performing initial write")
            writer = df.write.format("delta").mode("overwrite")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(full_path)
            return
        # Uperst
        if pk_col:
            logger.info(f"Performing Upsert on {file_path} using PK: {pk_col}")
            target_table = DeltaTable.forPath(spark, full_path)

            target_table.alias("target").merge(
                df.alias("updates"), f"target.{pk_col} = updates.{pk_col}"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            logger.warning(f"No PK defined for {file_path} Skip writing")
