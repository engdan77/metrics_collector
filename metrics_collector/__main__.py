from .__version__ import __version__
import os
import sys
from enum import Enum
from pathlib import Path
import appdirs
import typer
from metrics_collector.scheduler import MyScheduler
from metrics_collector.scheduler.tasks import default_initial_scheduled_tasks
from metrics_collector.web.service import WebServer
from loguru import logger
import logging


class LogLevel(str, Enum):
    INFO = 'INFO'
    DEBUG = 'DEBUG'


def filter_log(record):
    # return 'log' in record['message'] and record['name'].startswith('uvicorn')
    return False


def start_logging(data_dir, pkg_name, log_level):
    logging.getLogger('apscheduler').setLevel(logging.WARNING)
    logger.remove()
    logger.add(sys.stdout, level=getattr(logging, log_level), filter=filter_log)
    logger.add(f'{data_dir}{pkg_name}.log', rotation="1MB", retention="10 days", filter=filter_log)


def start_initial_loop(port):
    scheduler = MyScheduler(initials=default_initial_scheduled_tasks)
    WebServer((scheduler,), port=port)  # send services to be started with uvicorn


def start(port: int = typer.Option(5050, help="Port that Web Service use"), data_dir: str = typer.Option(None, help="Override default path for cache and configuration"), log_level: LogLevel = LogLevel.DEBUG):
    logger.info(f'Starting {__package__} {__version__}')
    pkg_name, *_ = __package__.split('.')
    default_app_dir = appdirs.user_data_dir()
    if data_dir:
        os.environ['DATA_DIR'] = data_dir
    if 'DATA_DIR' not in os.environ:
        logger.info(f'Creating {default_app_dir}')
        Path(default_app_dir).mkdir(parents=True, exist_ok=True)
        os.environ['DATA_DIR'] = default_app_dir
        data_dir = default_app_dir
    start_logging(data_dir, pkg_name, log_level=log_level)
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
