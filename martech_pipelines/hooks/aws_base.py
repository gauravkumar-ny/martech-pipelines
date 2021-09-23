from typing import Optional, Union, Tuple

import boto3
from functools import cached_property

from martech_pipelines.hooks.base import BaseHook


class AwsBaseHook(BaseHook):
    """
    Interact with AWS.
    This class is a thin wrapper around the boto3 python library.
    """

    default_conn_name = "aws_default"

    def __init__(
        self,
        aws_conn_id: Optional[str] = default_conn_name,
        region_name: Optional[str] = "ap-south-1",
        client_type: Optional[str] = None,
        resource_type: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.aws_conn_id = aws_conn_id
        self.client_type = client_type
        self.resource_type = resource_type
        self.region_name = region_name

        if not (self.client_type or self.resource_type):
            raise Exception("Either client_type or resource_type must be provided.")

    @cached_property
    def conn(self) -> Union[boto3.client, boto3.resource]:
        """
        Get the underlying boto3 client/resource (cached)
        :return: boto3.client or boto3.resource
        :rtype: Union[boto3.client, boto3.resource]
        """
        if self.client_type:
            return self.get_client_type(self.client_type, region_name=self.region_name)
        elif self.resource_type:
            return self.get_resource_type(
                self.resource_type, region_name=self.region_name
            )
        else:
            # Rare possibility - subclasses have not specified a client_type or resource_type
            raise NotImplementedError("Could not get boto3 connection!")

    def get_conn(self) -> Union[boto3.client, boto3.resource]:
        """
        Get the underlying boto3 client/resource (cached)
        :return: boto3.client or boto3.resource
        :rtype: Union[boto3.client, boto3.resource]
        """
        return self.conn

    def get_resource_type(
        self,
        resource_type: str,
        region_name: Optional[str] = None,
    ) -> boto3.resource:
        """Get the underlying boto3 resource using boto3 session"""
        session, endpoint_url = self._get_credentials(region_name)

        return session.resource(resource_type, endpoint_url=endpoint_url)

    def get_client_type(
        self,
        client_type: str,
        region_name: Optional[str] = None,
    ) -> boto3.client:
        """Get the underlying boto3 client using boto3 session"""
        session, endpoint_url = self._get_credentials(region_name)

        return session.client(client_type, endpoint_url=endpoint_url)

    def _get_credentials(
        self, region_name: Optional[str]
    ) -> Tuple[boto3.session.Session, Optional[str]]:

        if not self.aws_conn_id:
            session = boto3.session.Session(region_name=region_name)
            return session, None

        self.log.info("AWS Connection: aws_conn_id=%s", self.aws_conn_id)

        try:
            # Fetch the connection object
            connection_object = self.get_connection(self.aws_conn_id)
            extra_config = connection_object.extra_dejson
            endpoint_url = extra_config.get("host")
            region_name = extra_config.get("region_name")
            if connection_object.login:
                aws_access_key_id = connection_object.login
                aws_secret_access_key = connection_object.password
                self.log.info("Credentials retrieved from login")
                session = boto3.session.Session(
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=region_name,
                )
                return session, endpoint_url
            else:
                session = boto3.session.Session(region_name=region_name)
                return session, None
        except Exception as e:
            print(e)
            self.log.warning("Unable to use Airflow Connection for credentials.")

        self.log.info(
            "Creating session using boto3 region_name=%s",
            region_name,
        )
        session = boto3.session.Session(region_name=region_name)
        return session, None
