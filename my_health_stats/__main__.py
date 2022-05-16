import asyncio
import os
import sys
from pathlib import Path

import pandas
import datetime
from my_health_stats.extract.apple import AppleHealthExtract
from my_health_stats.extract.garmin import GarminExtract
from my_health_stats.utils import get_past_days
from my_health_stats.transform.base import GarminAppleTransform
from my_health_stats.load.graph import GarminAppleLoadGraph, GraphFormat
from scheduler import MyScheduler
from scheduler.tasks import default_initial_scheduled_tasks
from web.service import WebServer
from loguru import logger
import logging


def start_initial_loop():
    scheduler = MyScheduler(initials=default_initial_scheduled_tasks)
    web_server = WebServer((scheduler,))  # send services to be started with uvicorn


def get_data(number_of_days=1800):
    ah = AppleHealthExtract('../data/export.zip')
    g = GarminExtract(os.getenv('USERNAME'), os.getenv('PASSWORD'))

    for i, day in enumerate(get_past_days(number_of_days, offset=0)):
        for f in (g, ah):
            logger.debug(f'Processing {i}/{number_of_days} ')
            result = f.get_data(day)
            print(result)


def test_graphs():
    # get_data(20)
    apple_bytes = Path('../data/export.zip').read_bytes()
    ah = AppleHealthExtract(apple_bytes)
    g = GarminExtract(os.getenv('USERNAME'), os.getenv('PASSWORD'))
    _ = ah.get_data('2021-01-01')
    _ = g.get_data('2021-01-01')
    # df_ah = ah.to_df()
    # df_g = g.to_df()
    # df = pd.concat([df_ah, df_g])

    x = GarminAppleTransform(ah, g)

    # x.process_pipeline(datetime.date(2022, 1, 1), datetime.date(2022, 3, 1))
    print(x)

    graph_loader = GarminAppleLoadGraph(x, datetime.date(2021, 1, 1), datetime.date(2021, 3, 1))

    for graph_bytes in graph_loader.get_all_graphs(GraphFormat.html):
        Path('/tmp/a.html').write_text(graph_bytes)
        print('next')


def main():
    logger.remove()
    logger.add(sys.stdout, level=logging.DEBUG)
    start_initial_loop()


if __name__ == '__main__':
    main()
