from my_health_stats.applehealth import AppleHealth
from my_health_stats.garmin import MyGarmin
from my_health_stats.utils import get_past_days
import os
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    ah = AppleHealth('./data/health_data_2022.zip')
    g = MyGarmin(os.getenv('USERNAME'), os.getenv('PASSWORD'))
    number_of_days = 90

    for i, day in enumerate(get_past_days(number_of_days)):
        for f in (ah, g):
            logger.debug(f'Processing {d} / {number_of_days} ')
            result = f.get_data(day)
            print(result)


if __name__ == '__main__':
    main()
