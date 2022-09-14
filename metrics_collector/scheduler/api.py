import dataclasses
from enum import Enum
from typing import Optional, List, Dict, Tuple, TypedDict, Annotated
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
from asyncio.events import AbstractEventLoop
import importlib
import sys
import warnings
from loguru import logger

# Used to overcome "found in sys.modules after import of package .."
from metrics_collector.helper import import_item
from metrics_collector.scheduler.base import AsyncService, BaseAction

if not sys.warnoptions:  # allow overriding with `-W` option
    warnings.filterwarnings("ignore", category=RuntimeWarning, module="runpy")


class ActionType(str, Enum):
    Email = 'Email'
    Cache = 'Cache'


class ScheduleParams(TypedDict):
    """
    Typing for Schedule Params.
    More info here https://apscheduler.readthedocs.io/en/3.x/modules/triggers/cron.html
    """
    year: int | str
    month: int | str
    day: int | str
    day_of_week: int | str
    hour: int | str
    minute: int | str


@dataclasses.dataclass
class EmailAction(BaseAction):
    """Used as part of the schedule configuration"""
    to_email: str
    subject: str
    body: str

    def __format__(self, format_spec):
        s = self.shorten
        return f'{s(self.to_email)}, {s(self.subject)}'


@dataclasses.dataclass
class CacheAction(BaseAction):

    def __format__(self, format_spec):
        return ''


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

    @classmethod
    def verify_job(cls, schedule_params: ScheduleParams, temp_scheduler=AsyncIOScheduler) -> tuple[Annotated[bool, "success"], Annotated[str, "message"] | None]:
        """Method for verifying schedule params"""
        try:
            job = temp_scheduler().add_job(lambda: None, 'cron', **schedule_params)
        except ValueError as e:
            logger.warning(f'wrong schedule param {e}')
            return False, str(e)
        else:
            job.remove()
            return True, None

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
