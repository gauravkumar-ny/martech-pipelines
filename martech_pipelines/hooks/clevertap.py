import json
from typing import Any, Dict

import requests
import tenacity

from martech_pipelines.hooks.http import HttpHook


class ClevertapHook(HttpHook):
    """
    Interact with Clevertap APIs
    for more information:
    ::. see also https://developer.clevertap.com/docs/api-overview
    """

    def __init__(self, conn_id: str, _retry_args: Dict = None):
        super().__init__(http_conn_id=conn_id)
        self.base_url = "https://api.clevertap.com"
        self.delete_base_url = "https://in1.api.clevertap.com"
        self._retry_args = _retry_args or dict(
            wait=tenacity.wait_exponential(),
            stop=tenacity.stop_after_attempt(3),
            retry=tenacity.retry_if_exception(requests.exceptions.ConnectionError),
        )

    def upload_user_profiles(self, payload: Dict[Any]) -> Any:
        """create or update user profiles in CleverTap."""
        self.method = "POST"
        payload = json.dumps(payload)
        headers = {"Content-Type": "application/json; charset=utf-8"}
        response = self.run_with_advanced_retry(
            endpoint="1/upload",
            data=payload,
            headers=headers,
            _retry_args=self._retry_args,
        )
        return response

    def delete_user_profile(self, payload: Dict) -> Any:
        """This endpoint deletes a user profile."""
        self.method = "POST"
        endpoint = self.delete_base_url + "/1/delete/profiles.json"
        payload = json.dumps(payload)
        headers = {"Content-Type": "application/json"}
        response = self.run_with_advanced_retry(
            endpoint=endpoint,
            data=payload,
            headers=headers,
            _retry_args=self._retry_args,
        )
        return response

    def upload_events(self, payload: Dict) -> Any:
        """Upload user events to Clevertap"""
        self.method = "POST"
        payload = json.dumps(payload)
        headers = {"Content-Type": "application/json; charset=utf-8"}
        response = self.run_with_advanced_retry(
            endpoint="1/upload",
            data=payload,
            headers=headers,
            _retry_args=self._retry_args,
        )
        return response
