import importlib
from loguru import logger
import sys


def import_item(item):
    if type(item) is str:
        *module, attr = item.split(".")
        if not module:
            f = globals()[attr]
        else:
            f = getattr(importlib.import_module(".".join(module)), attr)
    else:
        f = item
    return f


def asyncio_exception_handler(loop, context):
    print("asyncio_exception_handler called.")
    exception = context.get("exception", None)
    if exception:
        exc_info = (type(exception), exception, exception.__traceback__)
        if issubclass(exception.__class__, KeyboardInterrupt):
            sys.__excepthook__(*exc_info)
            return
        logger.error(f"Uncaught Exception: {exc_info[0].__name__}")
    else:
        logger.error(context["message"])
