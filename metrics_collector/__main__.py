import sys
import typer
from metrics_collector.scheduler import MyScheduler
from metrics_collector.scheduler.tasks import default_initial_scheduled_tasks
from metrics_collector.web.service import WebServer
from loguru import logger
import logging


def start_initial_loop(port):
    scheduler = MyScheduler(initials=default_initial_scheduled_tasks)
    WebServer((scheduler,), port=port)  # send services to be started with uvicorn


def start(port: int = 5050):
    logger.remove()
    logger.add(sys.stdout, level=logging.DEBUG)
    start_initial_loop(port)


def main():
    typer.run(start)


if __name__ == '__main__':
    main()
