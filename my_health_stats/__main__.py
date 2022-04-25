import pandas as pd

from my_health_stats.platform.apple import AppleHealthPlatform
from my_health_stats.platform.garmin import GarminPlatform
from my_health_stats.utils import get_past_days
from my_health_stats.transformer.datatransformer import GarminAppleTransformer
import os
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_data(number_of_days=1800):
    ah = AppleHealthPlatform('../data/export.zip')
    g = GarminPlatform(os.getenv('USERNAME'), os.getenv('PASSWORD'))

    for i, day in enumerate(get_past_days(number_of_days, offset=0)):
        for f in (g, ah):
            logger.debug(f'Processing {i}/{number_of_days} ')
            result = f.get_data(day)
            print(result)


def main():
    ah = AppleHealthPlatform('../data/export.zip')
    g = GarminPlatform(os.getenv('USERNAME'), os.getenv('PASSWORD'))
    _ = ah.get_data('2021-01-01')
    _ = g.get_data('2021-01-01')
    # df_ah = ah.to_df()
    # df_g = g.to_df()
    # df = pd.concat([df_ah, df_g])

    x = GarminAppleTransformer(ah, g)
    print(x)
    ...


if __name__ == '__main__':
    main()
