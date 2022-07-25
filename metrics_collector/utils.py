from datetime import datetime, timedelta, date
from typing import Generator
import datetime

from loguru import logger
from parsedatetime import Calendar

def get_past_days(number_days: int = 1, offset=0) -> Generator:
    today = datetime.now()
    for d in range(1, number_days):
        x = today - timedelta(days=d + offset)
        yield x.strftime('%Y-%m-%d')


def normalize_period(from_: date | str, to_: date | str, fmt: str = '%Y-%m-%d') -> list[date, date]:
    try:
        from_ = datetime.datetime.strptime(from_, fmt)
        to_ = datetime.datetime.strptime(to_, fmt)
    except TypeError:
        pass
    except ValueError:
        c = Calendar()  # attempt parse as human text e.g. "6 months ago"
        from_, to_ = date(*c.parse(from_)[0][:3]), date(*c.parse(to_)[0][:3])
    return from_, to_


def get_days_between(from_: str | datetime.date, to_: str | datetime.date, as_text=True, fmt='%Y-%m-%d'):
    from_, to_ = normalize_period(from_, to_)
    logger.info(f'extracting data for period {from_}, {to_}')
    days = (to_ - from_).days
    for day in range(days):
        r: datetime.date = from_ + timedelta(days=day)
        yield r.strftime(fmt) if as_text else r

