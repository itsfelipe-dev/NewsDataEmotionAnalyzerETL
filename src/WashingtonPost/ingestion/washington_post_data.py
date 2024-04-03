import requests
import json


class WashingtonPostData:
    def __init__(self) -> None:
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

    def get_query_params(self) -> dict:
        query = {
            "query": "prism://prism.query/site-articles-only,/world/",
            "limit": self.limit,
            "offset": 0,
        }
        return {"_website": "washpost", "query": json.dumps(query)}

    def extract_data(self, limit: int) -> list:
        self.limit = limit
        try:
            response = requests.get(
                self.url, params=self.get_query_params(), headers=self.headers
            )
            data = response.raise_for_status()
            self.write_data(data)
        except requests.exceptions.RequestException as err:
            print(err)
            return None

    def write_data(self, data: list) -> bool:
        try:
            print("ok data wrote")
            return True
        except Exception as err:
            print(f"Error while writing data: {err}")
            return False
        