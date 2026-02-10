import logging
import boto3
import json
from botocore.exceptions import ClientError
logger = logging.getLogger(__name__)

class JsonWriter():
    def __init__(self):
        self.s3_client = boto3.client("s3")
    def write(self, data: list, bucket: str, file_path: str) -> None:
        logger.info(f"Attempting to write JSON in: {bucket}/{file_path}")
        try:
            self.s3_client.put_object(
                Body=json.dumps(data, ensure_ascii=False),
                Bucket=bucket,
                Key=file_path,
                ContentType="application/json",
                ContentEncoding="utf-8",
            )
        except ClientError as e:
            logger.error(
                f"Error writing data JSON S3: {e}, path: {bucket}/{file_path}"
            )
            raise
