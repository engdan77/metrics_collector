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


def is_docker():
    cgroup = Path("/proc/self/cgroup")
    return Path('/.dockerenv').is_file() or cgroup.is_file() and cgroup.read_text().find("docker") > -1


def filter_log(record):
    filter_entry = ('log' in record['message'] and not record['name'].startswith('uvicorn')) or any(_ in record['name'] for _ in ('websocket', 'http'))
    display_entry = not filter_entry
    return display_entry


def start_logging(data_dir, log_level):
    pkg_name, *_ = __package__.split('.')
    default_app_dir = appdirs.user_data_dir()
    if data_dir:
        os.environ['DATA_DIR'] = data_dir
    if is_docker():
        data_dir = '/app/data/'
        print(f'Default app dir: {default_app_dir}')
    elif 'DATA_DIR' not in os.environ:
        logger.info(f'Creating {default_app_dir}')
        Path(default_app_dir).mkdir(parents=True, exist_ok=True)
        os.environ['DATA_DIR'] = default_app_dir
        data_dir = default_app_dir
    data_dir.removeprefix('/')
    logging_path = f'{data_dir}/{pkg_name}.log'
    print(f'Logging to {logging_path}')
    logging.getLogger('apscheduler').setLevel(logging.WARNING)
    logger.remove()
    logger.add(sys.stdout, level=getattr(logging, log_level), filter=filter_log)
    logger.add(logging_path, rotation="1MB", retention="10 days", filter=filter_log)


def start_initial_loop(port):
    scheduler = MyScheduler(initials=default_initial_scheduled_tasks)
    WebServer((scheduler,), port=port)  # send services to be started with uvicorn


def start(port: int = typer.Option(5050, help="Port that Web Service use"), data_dir: str = typer.Option(None, help="Override default path for cache and configuration"), log_level: LogLevel = LogLevel.DEBUG):
    start_logging(data_dir, log_level=log_level)
    logger.info(f'Starting {__package__} {__version__}')
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
