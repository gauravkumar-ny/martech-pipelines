import os
import time
from functools import cached_property
from tempfile import NamedTemporaryFile
from typing import Dict

import pandas as pd
import requests
import tenacity
from pandas import DataFrame

from martech_pipelines.hooks.aws_s3 import S3Hook
from martech_pipelines.hooks.http import HttpHook
from martech_pipelines.utils.date_time import current_date
from martech_pipelines.utils.helpers import chunkify


class NetcoreHook(HttpHook):
    """
    Interact with Clevertap APIs
    for more information:
    ::. see also https://docs.netcoresmartech.com/docs/add-bulk-contact-2
    """

    def __init__(self, conn_id, _retry_args: Dict = None):
        super(NetcoreHook, self).__init__(http_conn_id=conn_id)
        self.base_url = "http://api.netcoresmartech.com"
        self.api_key = None
        self.activity_api_key = None
        self.notify_email = "marketing.tech@nykaa.com"
        self.count = 0
        self.bucket_name = None
        self.s3 = S3Hook()
        self._retry_args = _retry_args or dict(
            wait=tenacity.wait_exponential(),
            stop=tenacity.stop_after_attempt(3),
            retry=tenacity.retry_if_exception(requests.exceptions.ConnectionError),
        )

    @cached_property
    def connection(self):
        conn = self.get_connection(self.http_conn_id)
        self.api_key = conn.extra_dejson.get("api_key")
        self.activity_api_key = conn.extra_dejson.get("activity_api_key")
        self.notify_email = conn.extra_dejson.get(
            "notifyemail", "marketing.tech@nykaa.com"
        )
        self.bucket_name = conn.extra_dejson.get("ip_whitelisted_bucket")
        conn.extra = "{}"
        return conn

    def upload_user_profiles(self, data_payload, list_id: int = None):
        if isinstance(data_payload, DataFrame):
            df = pd.DataFrame(data_payload)
        else:
            df = data_payload
        key = f"netcore_prive_data/{self.http_conn_id}_user_profile_{int(time.time() * 1000.0)}_{current_date()}_part_{self.count}.csv "
        self.s3.upload_df_to_s3(df, self.bucket_name, key)
        self.count += 1
        payload = f"path=https://{self.bucket_name}.s3.ap-south-1.amazonaws.com/{key}"
        if list_id is not None:
            payload += f"&listid={list_id}"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        endpoint = f"apiv2?type=contact&activity=bulkupload&apikey={self.api_key}&notifyemail={self.notify_email}"
        response = self.run_with_advanced_retry(
            endpoint=endpoint,
            data=payload,
            headers=headers,
            _retry_args=self._retry_args,
        )
        return response

    def upload_events(self, payload):
        if isinstance(payload, DataFrame):
            df = payload.to_dict(orient="records")
        elif isinstance(payload, dict):
            df = payload
        else:
            self.log.error("Payload should be either dict of list or pandas dataframe")
            return None
        responses = list()
        for chunk in self.__get_chunks(df):
            with NamedTemporaryFile(mode="r+", suffix=".csv") as tmp_csv:
                df = pd.DataFrame(chunk)
                df.to_csv(tmp_csv.name, index=False, header=True)
                payload = {}
                headers = {}
                files = [
                    ("data", ("netcore.csv", open(tmp_csv.name, "rb"), "text/csv"))
                ]
                endpoint = f"v1/activity/batchactivity/{self.activity_api_key}"
                responses.append(
                    self.run_with_advanced_retry(
                        endpoint=endpoint,
                        data=payload,
                        files=files,
                        headers=headers,
                        _retry_args=self._retry_args,
                    )
                )
        return responses

    def __get_chunks(self, data):
        size = len(data)
        i = 1
        chunk = data
        while self.__df_file_size(chunk) > 4500000:
            i += 1
            chunks = chunkify(data, int(size / i))
            chunk = next(chunks)
        self.log.info(f"setting API chunk size at {int(size / i)} records")
        return chunkify(data, int(size / i))

    @staticmethod
    def __df_file_size(data):
        with NamedTemporaryFile(mode="r+", suffix=".csv") as tmp_csv:
            df = pd.DataFrame(data)
            df.to_csv(tmp_csv.name, index=False, header=True)
            return os.stat(tmp_csv.name).st_size
