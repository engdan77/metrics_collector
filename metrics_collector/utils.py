import os
from datetime import datetime, timedelta, date
from typing import Generator
import datetime

from appdirs import user_data_dir
from loguru import logger
from parsedatetime import Calendar


def get_past_days(number_days: int = 1, offset=0) -> Generator:
    today = datetime.now()
    for d in range(1, number_days):
        x = today - timedelta(days=d + offset)
        yield x.strftime('%Y-%m-%d')


def normalize_period(from_: date | str, to_: date | str, fmt: str = '%Y-%m-%d') -> tuple[date, date]:
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


def normalize_date(d: date | str, fmt: str = '%Y-%m-%d') -> date:
    try:
        d = datetime.datetime.strptime(d, fmt).date()
    except TypeError:
        pass
    except ValueError:
        c = Calendar()  # attempt parse as human text e.g. "6 months ago"
        d = date(*c.parse(d)[0][:3])
    return d


def get_days_between(from_: str | datetime.date, to_: str | datetime.date, as_text=True, fmt='%Y-%m-%d'):
    from_, to_ = normalize_period(from_, to_)
    logger.info(f'extracting data for period {from_}, {to_}')
    days = (to_ - from_).days
    for day in range(days):
        r: datetime.date = from_ + timedelta(days=day)
        yield r.strftime(fmt) if as_text else r


def get_cache_dir():
    return os.getenv('CACHE_DIR', None) or user_data_dir(__package__)


def shorten(input_data, letters=8):
    return f'{input_data[:letters]}...' if len(input_data) >= letters else input_data