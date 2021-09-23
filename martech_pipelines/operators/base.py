from martech_pipelines.utils.log import LoggingMixin


class BaseOperator(LoggingMixin):
    def __init__(self) -> None:
        super().__init__()

    def execute(self) -> None:
        raise NotImplementedError
