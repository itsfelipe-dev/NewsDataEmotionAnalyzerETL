import requests
import logging
from includes.utils import SparkUtils
from includes.utils import get_env_conf
from datetime import datetime, timezone
from functools import wraps
import time
import threading

logger = logging.getLogger(__name__)
config = get_env_conf()


class RateLimiter:
    """Class to enforce a time-based quota limit."""

    last_call_time = 0
    lock = threading.Lock()

    @staticmethod
    def quota_limiter(quota_seconds: int = 12):
        """Decorator to enforce a function call rate limit."""

        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                with RateLimiter.lock:
                    elapsed_time = time.time() - RateLimiter.last_call_time
                    if elapsed_time < quota_seconds:
                        remaining_time = quota_seconds - elapsed_time
                        logger.warning(
                            f"Quota limit reached. Please wait {remaining_time:.2f} seconds."
                        )
                        for i in range(int(remaining_time), 0, -1):
                            print(f"Waiting... {i} seconds remaining", end="\r")
                            time.sleep(1)

                result = func(self, *args, **kwargs)
                with RateLimiter.lock:
                    RateLimiter.last_call_time = time.time()  # Update last call time
                return result

            return wrapper

        return decorator


class APIDataExtractor(object):
    def __init__(self, url: str, source: str) -> None:
        self.url = url
        self.bucket_folder = source
        self.sensitive_keys = {"api-key"}

    def build_ingestion_envelope(self, params, raw_response):
        return {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source_system": self.bucket_folder,
            "url": self.url,
            "request": params,
            "response_payload": raw_response,
        }

    def sanitize_params(self, params: dict) -> dict:
        return {k: v for k, v in params.items() if k not in self.sensitive_keys}

    @RateLimiter.quota_limiter(12)
    def get_data(self, params) -> dict:
        try:
            sanitize_params = self.sanitize_params(params)
            logger.info(
                f"Extracting data from {self.url}/{self.section} with params: {sanitize_params}"
            )

            response = requests.get(self.url, params=params, timeout=10)
            response.raise_for_status()

            return self.build_ingestion_envelope(
                params=sanitize_params, raw_response=response.json()
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"The request had invalid params: {self.url} - {e}")
            raise
