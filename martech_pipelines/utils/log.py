import logging
from logging import Logger


def set_logger_config(level=None):
    level = level or logging.INFO
    logging.basicConfig(
        format="%(asctime)s -  %(name)s - %(levelname)s - %(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
        level=level,
    )


class LoggingMixin:
    """Convenience super-class to have a logger configured with the class name"""

    def __init__(self):
        self._log = None
        set_logger_config()

    @property
    def log(self) -> Logger:
        """Returns a logger."""
        if self._log is not None:
            return self._log
        else:
            try:
                log4j_logger = spark._jvm.org.apache.log4j  # noqa
                self._log = log4j_logger.LogManager.getLogger(
                    self.__class__.__module__ + "." + self.__class__.__name__
                )
                return self._log
            except Exception as ex:
                self._log = logging.getLogger(
                    self.__class__.__module__ + "." + self.__class__.__name__
                )
                return self._log
