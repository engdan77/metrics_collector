import os
import sys
from enum import Enum

import typer
from metrics_collector.scheduler import MyScheduler
from metrics_collector.scheduler.tasks import default_initial_scheduled_tasks
from metrics_collector.web.service import WebServer
from loguru import logger
import logging


class LogLevel(str, Enum):
    INFO = 'INFO'
    DEBUG = 'DEBUG'


def start_initial_loop(port):
    scheduler = MyScheduler(initials=default_initial_scheduled_tasks)
    WebServer((scheduler,), port=port)  # send services to be started with uvicorn


def start(port: int = typer.Option(5050, help="Port that Web Service use"), data_dir: str = typer.Option(None, help="Override default path for cache and configuration"), log_level: LogLevel = LogLevel.INFO):
    if data_dir:
        os.environ['DATA_DIR'] = data_dir
    logging.getLogger('apscheduler').setLevel(logging.WARNING)
    logger.remove()
    logger.add(sys.stdout, level=getattr(logging, log_level))
    try:
        start_initial_loop(port)
    except AttributeError as e:
        if e.name == 'loaded':
            logger.info('Exiting')  # workaround supress message at exit
        else:
            raise


def main():
    typer.run(start)


if __name__ == '__main__':
    main()
