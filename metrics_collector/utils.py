import os
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Generator, Annotated
import datetime
from appdirs import user_data_dir
from loguru import logger
from loguru._file_sink import FileSink
from parsedatetime import Calendar

from inspect import isclass
from pkgutil import iter_modules
from pathlib import Path
from importlib import import_module
import inspect


def load_all_submodules(calling_package_name: Annotated[str | None, "send __name__ as argument"] = None):
    """iterate through the modules in the package and load it"""
    if calling_package_name:
        *_, pkg_name = calling_package_name.split('.')
        stack_frames = inspect.stack()
        stack_frames_sum = [(f.function, f.filename) for f in stack_frames]
        idx_func_called, *_ = [i for i, _ in enumerate(stack_frames_sum) if _[0] == 'load_all_submodules']
        package_file_path = stack_frames[idx_func_called + 1].filename
    else:
        package_file_path = __file__
        calling_package_name = __name__

    package_dir = Path(package_file_path).resolve().parent.as_posix()
    for (_, module_name, _) in iter_modules([package_dir]):
        # import the module and iterate through its attributes
        module = import_module(f"{calling_package_name}.{module_name}")
        for attribute_name in dir(module):
            attribute = getattr(module, attribute_name)

            if isclass(attribute):
                # Add the class to this package's variables
                globals()[attribute_name] = attribute


def get_past_days(number_days: int = 1, offset=0) -> Generator:
    today = datetime.now()
    for d in range(1, number_days):
        x = today - timedelta(days=d + offset)
        yield x.strftime("%Y-%m-%d")


def normalize_period(
    from_: date | str, to_: date | str, fmt: str = "%Y-%m-%d"
) -> tuple[date, date]:
    try:
        from_ = datetime.datetime.strptime(from_, fmt)
        to_ = datetime.datetime.strptime(to_, fmt)
    except TypeError:
        pass
    except ValueError:
        c = Calendar()  # attempt parse as human text e.g. "6 months ago"
        from_ = from_.isoformat() if isinstance(from_, datetime.datetime) else from_
        to_ = from_.isoformat() if isinstance(to_, datetime.datetime) else to_
        from_, to_ = date(*c.parse(from_)[0][:3]), date(*c.parse(to_)[0][:3])
    return from_, to_


def normalize_date(d: date | str, fmt: str = "%Y-%m-%d") -> date:
    try:
        d = datetime.datetime.strptime(d, fmt).date()
    except TypeError:
        pass
    except ValueError:
        c = Calendar()  # attempt parse as human text e.g. "6 months ago"
        d = date(*c.parse(d)[0][:3])
    return d


def get_days_between(
    from_: str | datetime.date, to_: str | datetime.date, as_text=True, fmt="%Y-%m-%d"
):
    from_, to_ = normalize_period(from_, to_)
    logger.info(f"extracting data for period {from_}, {to_}")
    days = (to_ - from_).days
    for day in range(days):
        r: datetime.date = from_ + timedelta(days=day)
        yield r.strftime(fmt) if as_text else r


def get_data_dir():
    return os.getenv("DATA_DIR", None) or user_data_dir(__package__)


def shorten(input_data, letters=8):
    return f"{input_data[:letters]}..." if len(input_data) >= letters else input_data


def get_file_sink_from_logger():
    for _, handler in logger._core.handlers.items():
        if isinstance(handler._sink, FileSink):
            return handler._sink._file.name


def get_last_log_lines(number_of_lines=20):
    f = get_file_sink_from_logger()
    lines = list(reversed([_ for _ in Path(f).read_text().split('\n') if not '/log' in _]))[:number_of_lines]
    return lines
