import json
from json import JSONDecodeError
from typing import Dict
from functools import lru_cache

from martech_pipelines.hooks.aws_secret_manager import SecretsManagerHook

from martech_pipelines.utils.log import LoggingMixin
from martech_pipelines.utils.config import get_secret_name


class Connection(LoggingMixin):
    """
    Placeholder to store information about connections
    """

    def __init__(
        self,
        conn_id=None,
        description=None,
        host=None,
        login=None,
        password=None,
        schema=None,
        port=None,
        extra=None,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.description = description
        self.host = host
        self.login = login
        self.password = password
        self.schema = schema
        self.port = port
        self.extra = extra

    @property
    def extra_dejson(self) -> Dict:
        """Returns the extra property by deserializing json."""
        obj = {}
        if self.extra and isinstance(self.extra, str):
            try:
                obj = json.loads(self.extra)

            except JSONDecodeError:
                self.log.exception(
                    "Failed parsing the json for conn_id %s", self.conn_id
                )
        elif self.extra and isinstance(self.extra, Dict):
            obj = self.extra
        return obj

    @classmethod
    def get_connection(cls, conn_id) -> "Connection":
        """
        Get connection by conn_id.
        :param conn_id: connection id
        :return: connection
        """
        if isinstance(conn_id, Connection):
            return conn_id
        cfg = get_connection_from_secret_manager()
        if cfg is None or cfg.get(conn_id) is None:
            raise Exception(f"Connection Not found : {conn_id}")
        return Connection(**cfg[conn_id])


@lru_cache()
def get_connection_from_secret_manager(secret_name: str = None) -> Dict:
    secrets = SecretsManagerHook()
    secret_name = secret_name or get_secret_name()
    cfg = secrets.get_secret_as_dict(secret_name=secret_name)
    return cfg
