from my_health_stats.apple_health import AppleHealth
from my_health_stats.garmin import MyGarmin
import os
import logging

logging.basicConfig(level=logging.DEBUG)

def main():
    # ah = AppleHealth('./data/health_data_2022.zip')
    # ah.to_json('health.json')

    g = MyGarmin(os.getenv('USERNAME'), os.getenv('PASSWORD'))
    g.get_data()

if __name__ == '__main__':
    main()
