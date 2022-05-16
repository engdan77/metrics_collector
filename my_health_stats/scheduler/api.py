from typing import Optional, List, Dict, Tuple
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
from asyncio.events import AbstractEventLoop
import importlib
import sys
import warnings
from loguru import logger

# Used to overcome "found in sys.modules after import of package .."
from my_health_stats.helper import import_item
from my_health_stats.scheduler.base import AsyncService

if not sys.warnoptions:  # allow overriding with `-W` option
    warnings.filterwarnings("ignore", category=RuntimeWarning, module="runpy")


class MyScheduler(AsyncService):
    def __init__(
        self,
        initials: List[Tuple[str, str, Dict]],
        schedule_queue: Optional[asyncio.Queue] = None,
        loop: AbstractEventLoop = None
    ) -> None:
        self.loop = loop
        self.scheduler = AsyncIOScheduler()

        if schedule_queue is None:
            self.queue = schedule_queue
        else:
            self.queue = asyncio.Queue()

        self.add_initials(initials)

    def start(self):
        if self.loop:
            self.loop.create_task(self.start_async())
        else:
            self.scheduler.start()

    async def start_async(self):
        self.scheduler.start()

    def add_task(self, _func, _type, _args=[], _kwargs={}, **kwargs):
        logger.info(f"adding scheduling for {_func} with {kwargs}")
        f = import_item(_func)
        # add job function f, calling with _args and _kwargs while **kwargs for trigger options
        self.scheduler.add_job(
            f, _type, _args, _kwargs, **kwargs
        )  # special trick to allow calling attr within other package

    def add_initials(self, initials):
        for _func, _type, kwargs in initials:
            self.add_task(_func, _type, **kwargs)
