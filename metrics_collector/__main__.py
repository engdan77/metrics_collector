import os
import sys
import typer
from metrics_collector.extract.garmin import GarminExtract, GarminExtractParameters
from metrics_collector.utils import get_past_days
from metrics_collector.scheduler import MyScheduler
from metrics_collector.scheduler.tasks import default_initial_scheduled_tasks
from metrics_collector.web.service import WebServer
from loguru import logger
import logging


def start_initial_loop(port):
    scheduler = MyScheduler(initials=default_initial_scheduled_tasks)
    WebServer((scheduler,), port=port)  # send services to be started with uvicorn


def get_data(number_of_days=1800):
    # ah = AppleHealthExtract('../data/export.zip')
    g = GarminExtract(GarminExtractParameters(os.getenv('USERNAME'), os.getenv('PASSWORD')))

    for i, day in enumerate(get_past_days(number_of_days, offset=0)):
        for f in (g, ah):
            logger.debug(f'Processing {i}/{number_of_days} ')
            result = f.get_data(day)
            print(result)


def start(port: int = 5050):
    logger.remove()
    logger.add(sys.stdout, level=logging.DEBUG)
    start_initial_loop(port)


def main():
    typer.run(start)


if __name__ == '__main__':
    main()
