from datetime import datetime, timedelta
from typing import Generator


def get_past_days(number_days: int = 1) -> Generator:
    today = datetime.now()
    for d in range(1, number_days):
        x = today - timedelta(days=d)
        yield x.strftime('%Y-%m-%d')