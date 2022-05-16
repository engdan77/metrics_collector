from datetime import datetime
from loguru import logger

default_initial_scheduled_tasks = [
    ["my_health_stats.scheduler.tasks.ping", "interval", {"seconds": 15, "id": "tick"}],
]


async def ping():
    print('ping')
    logger.debug("Pong! The time is: %s" % datetime.now())