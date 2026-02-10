from ingestion.adapters.nytimes import NyTimes
from ingestion.adapters.theguardian import TheGuardian
from ingestion.adapters.washingtonpost import WashingtonPost
from writers.json_writer import JsonWriter
from includes.utils import get_env_conf
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

config = get_env_conf()


class BronzeOrchestaror(object):
    ADAPTERS = {"nytimes": NyTimes, "theguardian": TheGuardian}

    def __init__(self) -> None:
        self.bucket_name = config["s3"]["bucket_zones"]["bronze_zone"]
        self.environment = config["environment"]

    def get_file_path(self, section: str, page: int, source: str) -> str:
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return f"{self.environment}/{source}/ingestion_date={ingestion_date}/section={section}/page={page}.json"

    def run(self, source):

        sections = config["data_sources"][source]["sections"]

        writer = JsonWriter()
        for section in sections:
            adapter = self.ADAPTERS[source](
                category_post=section,
                page_size=10,
            )
            total_pages = 3
            for page in range(1, total_pages + 1):  
                file_path = self.get_file_path(
                    page=page, section=section, source=source
                )

                params = adapter.get_query_params(offset=page)
                try:
                    data = adapter.get_data(params)
                    if not data:
                        logger.warning(
                            f"No data returned for {source} - {section} - page {page}"
                        )
                        continue
                    writer.write(
                        data=data,
                        bucket=self.bucket_name,
                        file_path=file_path,
                    )
                    logger.info(
                        f"Successfully stored data in: {self.bucket_name}/{file_path} length: {len(data)}"
                    )

                except Exception as e:
                    logger.error(
                        f"Can't store data in: {self.bucket_name}/{file_path} - {e}"
                    )
