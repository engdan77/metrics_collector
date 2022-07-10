from datetime import datetime, timedelta, date
from typing import Generator
import datetime


def get_past_days(number_days: int = 1, offset=0) -> Generator:
    today = datetime.now()
    for d in range(1, number_days):
        x = today - timedelta(days=d + offset)
        yield x.strftime('%Y-%m-%d')


def get_days_between(from_: str | datetime.date, to_: str | datetime.date, as_text=True, fmt='%Y-%m-%d'):
    try:
        from_ = datetime.datetime.strptime(from_, fmt)
        to_ = datetime.datetime.strptime(to_, fmt)
    except TypeError:
        pass
    days = (to_ - from_).days
    for day in range(days):
        r: datetime.date = from_ + timedelta(days=day)
        yield r.strftime(fmt) if as_text else r

