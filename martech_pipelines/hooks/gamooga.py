import json
from typing import Dict, Any

import jwt
import requests
import tenacity

from martech_pipelines.hooks.http import HttpHook
from functools import cached_property


class GamoogaHook(HttpHook):
    """
    Interact with Gamooga APIs
    """

    def __init__(self, conn_id, company_id: str = None, secret_key: str = None):
        super().__init__(http_conn_id=conn_id)
        self.company_id = company_id
        self.secret_key = secret_key
        self.retry_args = dict(
            wait=tenacity.wait_exponential(),
            stop=tenacity.stop_after_attempt(3),
            retry=tenacity.retry_if_exception(requests.exceptions.ConnectionError),
        )
        self.first_call = self.connection

    @cached_property
    def connection(self):
        conn_obj = self.get_connection(self.http_conn_id)
        if not self.company_id:
            self.company_id = conn_obj.login
        if not self.secret_key:
            self.secret_key = conn_obj.password
        conn_obj.login = None
        conn_obj.password = None
        return conn_obj

    def upload_user_profiles(
        self, properties: str, schema: str, retry_args=None
    ) -> Any:
        """Send user properties to Gamooga in Bulk. Max 1000 in single batch"""
        self.method = "POST"
        data = {
            "props": json.loads(properties),
            "c": self.company_id,
            "prop_types": json.loads(schema),
        }
        payload = json.dumps(data)
        retry = retry_args or self.retry_args
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = self.run_with_advanced_retry(
            endpoint="bulkvpr/", headers=headers, data=payload, _retry_args=retry
        )
        return response

    def upload_events(self, payload: str, retry_args: Dict[Any, Any] = None) -> Any:
        """Send bulk events to Gamooga. Max batch Size 1000 events"""
        self.method = "POST"
        jwt_token = self.__encoded_data__(json.loads(payload))
        data = {"jwt": jwt_token, "c": self.company_id}
        payload = json.dumps(data)
        retry = retry_args or self.retry_args
        response = self.run_with_advanced_retry(
            endpoint="bev/", data=payload, _retry_args=retry
        )
        return response

    def __encoded_data__(self, data):
        payload = {"payload": data}
        token = ""
        if data:
            token = jwt.encode(payload, self.secret_key, algorithm="HS256")
        return token
