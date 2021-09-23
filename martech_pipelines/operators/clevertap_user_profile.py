from typing import Union

import numpy as np
import pandas as pd
from pandas import DataFrame

import martech_pipelines.utils.data_sanity_helpers
from martech_pipelines.hooks.aws_s3 import S3Hook
from martech_pipelines.hooks.clevertap import ClevertapHook
from martech_pipelines.operators.base import BaseOperator
from martech_pipelines.utils.data_sanity_helpers import *


class ClevertapProfileOperator(BaseOperator):
    TOTAL_RECORD_PROCESSED = 0
    TOTAL_RECORD_SEND = 0
    TOTAL_INVALID_RECORD = 0

    def __init__(
        self,
        conn_id: str,
        df: DataFrame = None,
        s3_bucket: str = None,
        s3_key: str = None,
        attributes=None,
        swap_key_map: Optional[Dict] = None,
        data_type_map: Optional[Dict] = None,
        aws_conn_id: str = None,
        payload_size: int = 1000,
        identity="customer_id",
        **kwargs,
    ):
        super(ClevertapProfileOperator, self).__init__()
        if not df or not s3_bucket and not s3_key:
            self.log.error(
                "Either s3 path or pandas dataframe should be provided for payload"
            )
            raise Exception("data for payload not passed")
        self.conn_id = conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.df = df
        self.attributes = attributes
        self.swap_key_map = swap_key_map or {}
        self.data_type_map = data_type_map or {}
        self.payload_size = payload_size
        self.identity = identity
        self.parameters = kwargs.pop("task_params", {})
        self.transformation_fn: callable = (
            getattr(
                martech_pipelines.utils.data_sanity_helpers,
                self.parameters.get("transformation_fn", ""),
                None,
            )
            if self.parameters.get("transformation_fn") is None
            or isinstance(self.parameters.get("transformation_fn"), str)
            else self.parameters.get("transformation_fn")
        )

    def execute(self) -> None:
        chunk_completed = 0
        ct = ClevertapHook(conn_id=self.conn_id)
        for payload in self.payload_generator():
            response = ct.upload_user_profiles(payload)
            self.log.info(response.json())
            chunk_completed += 1
            self.log.info(f"CHUNKS_COMPLETED : {chunk_completed}")

    def payload_generator(self):
        for pd_chunk in self.get_data_df():
            self.TOTAL_RECORD_PROCESSED = self.TOTAL_RECORD_PROCESSED + len(pd_chunk)
            pd_chunk = pd_chunk.where(pd.notnull(pd_chunk), None)
            pd_chunk = pd_chunk.to_dict(orient="records")
            pd_chunk = list(map(self.transform_payload, pd_chunk))
            pd_chunk = list(filter(None, pd_chunk))
            self.TOTAL_RECORD_SEND += len(pd_chunk)
            payload = {"d": pd_chunk}
            yield payload

    def get_data_df(self):
        if self.df is not None:
            chunk_counts = len(self.df) / self.payload_size
            yield np.array_split(self.df, chunk_counts)
        else:
            s3 = S3Hook(aws_conn_id=self.aws_conn_id)
            if not s3.check_for_key(key=self.s3_key, bucket_name=self.s3_bucket):
                self.log.warning(f"No s3 file found at {self.s3_key}... Exiting")
                return
            obj = s3.get_key(key=self.s3_key, bucket_name=self.s3_bucket)
            yield pd.read_csv(obj.get()["Body"], chunksize=self.payload_size)

    def transform_payload(self, data: Dict) -> Union[Dict, None]:
        identity = identity_field_check(data, self.identity)
        if identity is None:
            self.log.warning(f"No identity field: {data}")
            self.TOTAL_INVALID_RECORD = self.TOTAL_INVALID_RECORD + 1
            return None
        res = {"type": "profile", "identity": identity}

        if self.transformation_fn is not None:
            data = self.transformation_fn(data, self.parameters)

        data = data_type_transformation(data, self.data_type_map)
        # remove extra columns from s3 file.
        if self.attributes is not None:
            data = extract_attributes(data, self.attributes)

        data = swap_key_name(data, self.swap_key_map)
        res["profileData"] = data
        return res
