from typing import Protocol


class Logger(Protocol):
    def debug(self, msg, *args, **kwargs): ...  # noqa: ANN201, ANN001, ANN002, ANN003

    def info(self, msg, *args, **kwargs): ...  # noqa: ANN201, ANN001, ANN002, ANN003

    def warning(self, msg, *args, **kwargs): ...  # noqa: ANN201, ANN001, ANN002, ANN003

    def warn(self, msg, *args, **kwargs): ...  # noqa: ANN201, ANN001, ANN002, ANN003

    def error(self, msg, *args, **kwargs): ...  # noqa: ANN201, ANN001, ANN002, ANN003

    def exception(self, msg, *args, exc_info=True, **kwargs): ...  # noqa: ANN201, ANN001, ANN002, ANN003
