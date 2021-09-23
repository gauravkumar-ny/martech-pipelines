from martech_pipelines.utils.log import LoggingMixin

from martech_pipelines.connection import Connection


class BaseHook(LoggingMixin):
    def __init__(self):
        super(BaseHook, self).__init__()

    @classmethod
    def get_connection(cls, conn_id: str):
        """
        Get connection, given connection id.
        :param conn_id: connection id
        :return: connection
        """
        conn = Connection.get_connection(conn_id)
        return conn
