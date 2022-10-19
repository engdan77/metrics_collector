class MetricsBaseException(Exception):
    """The base exception class"""
    ...


class MetricsExtractException(MetricsBaseException):
    """Occurs if e.g. fs fails connect to ftp"""
    ...
