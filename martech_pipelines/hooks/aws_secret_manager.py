import base64
import json
from typing import Union

import boto3
from functools import cached_property


class SecretsManagerHook:
    """
    Interact with Amazon SecretsManager Service.
    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.
    .. see also::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        self.client_type = "secretsmanager"
        self.region_name = "ap-south-1"

    @cached_property
    def get_conn(self):
        session = boto3.session.Session()
        client = session.client(
            service_name=self.client_type, region_name=self.region_name
        )
        return client

    def get_secret(self, secret_name: str) -> Union[str, bytes]:
        """
        Retrieve secret value from AWS Secrets Manager as a str or bytes
        reflecting format it stored in the AWS Secrets Manager
        :param secret_name: name of the secrets.
        :type secret_name: str
        :return: Union[str, bytes] with the information about the secrets
        :rtype: Union[str, bytes]
        """
        # Depending on whether the secret is a string or binary, one of
        # these fields will be populated.
        get_secret_value_response = self.get_conn.get_secret_value(SecretId=secret_name)
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
        else:
            secret = base64.b64decode(get_secret_value_response["SecretBinary"])
        return secret

    def get_secret_as_dict(self, secret_name: str) -> dict:
        """
        Retrieve secret value from AWS Secrets Manager in a dict representation
        :param secret_name: name of the secrets.
        :type secret_name: str
        :return: dict with the information about the secrets
        :rtype: dict
        """
        return json.loads(self.get_secret(secret_name))
