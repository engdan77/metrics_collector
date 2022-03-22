from my_health_stats.applehealth import AppleHealth
from my_health_stats.garmin import MyGarmin
import os
import logging

logging.basicConfig(level=logging.DEBUG)

def main():
    ah = AppleHealth('./data/health_data_2022.zip')
    # ah.to_json('health.json')
    i = ah.get_data('2021-08-07')
    print(i)

    g = MyGarmin(os.getenv('USERNAME'), os.getenv('PASSWORD'))
    j = g.get_data('2021-08-07')
    print(j)

if __name__ == '__main__':
    main()
