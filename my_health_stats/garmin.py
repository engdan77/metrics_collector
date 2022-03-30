from collections import defaultdict

from garminconnect import Garmin, GarminConnectConnectionError
from loguru import logger
from my_health_stats.base import DaysActivities, BaseService
from datetime import date, datetime
import time


class MyGarmin(BaseService):

    def __init__(self, username, password):
        self.api = Garmin(username, password)
        self.logged_in = False

    def login(self, retries=5, timer=10):
        for _ in range(1, retries + 1):
            try:
                self.logged_in = self.api.login()
                logger.debug('Successfully logged in to GarminConnect')
                break
            except GarminConnectConnectionError:
                sleep_time = _ * timer
                logger.warning(f'Failed attempt {_}, sleep for {sleep_time} secs')
                time.sleep(sleep_time)

    def get_data_from_service(self, date_: str) -> DaysActivities:
        if not self.logged_in:
            self.login()
        result = defaultdict(lambda: defaultdict(dict))
        d = date_
        activities = self.api.get_activities_by_date(d, d, None)
        key_unit = {'distance': 'meters',
                    'duration': 'seconds',
                    'calories': 'cal',
                    'maxHR': 'bpm',
                    'averageHR': 'bpm',
                    'steps': 'count'
                    }
        result[d] = {}
        for i, a in enumerate(activities, start=1):
            logger.debug(f'Activity {i}')
            for key, unit in key_unit.items():
                activity_name = f"{a['activityType']['typeKey'].lower()}_{key}"
                if activity_name not in result[d]:
                    result[d][activity_name] = {}
                result[d][activity_name]['unit'] = unit
                values = result[d][activity_name].get("value", [])
                values.append(a[key])
                result[d][activity_name]["value"] = values
        return dict(result)
