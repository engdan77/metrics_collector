import pandas
import datetime
from my_health_stats.extract.apple import AppleHealthExtract
from my_health_stats.extract.garmin import GarminExtract
from my_health_stats.utils import get_past_days
from my_health_stats.transform.base import GarminAppleTransform
from my_health_stats.load.graph import GarminAppleLoadGraph
import os
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_data(number_of_days=1800):
    ah = AppleHealthExtract('../data/export.zip')
    g = GarminExtract(os.getenv('USERNAME'), os.getenv('PASSWORD'))

    for i, day in enumerate(get_past_days(number_of_days, offset=0)):
        for f in (g, ah):
            logger.debug(f'Processing {i}/{number_of_days} ')
            result = f.get_data(day)
            print(result)


def main():
    # get_data(20)
    ah = AppleHealthExtract('../data/export.zip')
    g = GarminExtract(os.getenv('USERNAME'), os.getenv('PASSWORD'))
    _ = ah.get_data('2021-01-01')
    _ = g.get_data('2021-01-01')
    # df_ah = ah.to_df()
    # df_g = g.to_df()
    # df = pd.concat([df_ah, df_g])

    x = GarminAppleTransform(ah, g)

    # x.process_pipeline(datetime.date(2022, 1, 1), datetime.date(2022, 3, 1))
    print(x)

    graph = GarminAppleLoadGraph(x, datetime.date(2021, 1, 1), datetime.date(2021, 3, 1))
    print(graph)
    ...


if __name__ == '__main__':
    main()
