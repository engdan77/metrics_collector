import asyncio
import os
import sys
from pathlib import Path
import datetime
import typer
from metrics_collector.extract.apple import AppleHealthExtract, AppleHealthExtractParameters
from metrics_collector.extract.garmin import GarminExtract, GarminExtractParameters
from metrics_collector.utils import get_past_days
from metrics_collector.transform.transformers import GarminAppleTransform
from metrics_collector.load.graph import GarminAppleLoadGraph
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


def test_graphs():
    # get_data(20)
    apple_bytes = Path('../data/export.zip').read_bytes()
    ah_params = AppleHealthExtractParameters(apple_bytes)
    ah = AppleHealthExtract(ah_params)
    g_params = GarminExtractParameters(os.getenv('USERNAME'), os.getenv('PASSWORD'))
    g = GarminExtract(g_params)
    _ = ah.get_data('2021-01-01')
    _ = g.get_data('2021-01-01')
    # df_ah = ah.to_df()
    # df_g = g.to_df()
    # df = pd.concat([df_ah, df_g])

    x = GarminAppleTransform(ah, g)

    # x.process_pipeline(datetime.date(2022, 1, 1), datetime.date(2022, 3, 1))
    print(x)

    graph_loader = GarminAppleLoadGraph(x, datetime.date(2021, 1, 1), datetime.date(2021, 3, 1))

    # for graph_bytes in graph_loader.get_all_graphs(GraphFormat.html):
    #     Path('/tmp/a.html').write_text(graph_bytes)
    #     print('next')


def start(port: int = 5050):
    logger.remove()
    logger.add(sys.stdout, level=logging.DEBUG)
    start_initial_loop(port)


def main():
    typer.run(start)


if __name__ == '__main__':
    main()
