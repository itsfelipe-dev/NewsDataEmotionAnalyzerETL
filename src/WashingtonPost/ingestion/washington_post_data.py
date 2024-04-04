import requests
import json
import logging
import yaml
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

with open("./includes/config.yml", "r") as file:
    config = yaml.safe_load(file)


class WashingtonPostData(object):
    def __init__(self):
        self.url = "https://www.washingtonpost.com/prism/api/prism-query"
        self.headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Te": "trailers",
        }
        self.filename = "world.json"
        self.category = "world_articles"
        self.file_path = f"{self.category}/{self.filename}"
        self.save_path = config["BUCKET"]["BRONZE_ZONE"]
        self.bucket_folder = config["BUCKET"]["FOLDER"]

    def get_query_params(self, limit: int) -> dict:
        query = {
            "query": "prism://prism.query/site-articles-only,/world/",
            "limit": limit,
            "offset": 0,
        }
        return {"_website": "washpost", "query": json.dumps(query)}

    def extract_data(self, limit: int) -> None:
        params = self.get_query_params(limit)

        try:
            logging.info(f"Attempting to extract data from {self.url}")
            response = requests.get(self.url, params=params, headers=self.headers)
            response.raise_for_status()
            self.write_data(
                data=response.json(),
                bucket=self.save_path + "/",
                file_path=self.bucket_folder + self.file_path,
            )

        except requests.exceptions.RequestException as err:
            logging.error(f"The request had invalid params: {err}")

    def write_data(self, data: list, bucket: str, file_path: str) -> None:
        s3_client = boto3.client("s3")
        logging.info(f"Attempting to write data to {bucket}")
        try:
            s3_client.put_object(Body=json.dumps(data), Bucket=bucket, Key=file_path)
        except ClientError as e:
            logging.error(f"Error writing data in S3: {e}")
