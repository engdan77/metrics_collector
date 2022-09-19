import dataclasses
import json
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Annotated
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
from asyncio.events import AbstractEventLoop
import sys
import warnings
from loguru import logger

# Used to overcome "found in sys.modules after import of package .."
from metrics_collector.helper import import_item
from metrics_collector.scheduler.base import AsyncService, BaseAction, BaseScheduleParams, ActionType
from metrics_collector.utils import shorten, get_cache_dir

if not sys.warnoptions:  # allow overriding with `-W` option
    warnings.filterwarnings("ignore", category=RuntimeWarning, module="runpy")


@dataclasses.dataclass
class ScheduleParams(BaseScheduleParams):
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

    def __format__(self, format_spec):
        return f"Y:{self.year} M:{self.month} D:{self.day} DoW: {self.day_of_week} H:{self.hour} M:{self.minute}"


@dataclasses.dataclass
class ScheduleConfig:
    dag_name: str
    from_: str
    to_: str
    extract_params: dict
    schedule_params: ScheduleParams
    action_type: ActionType
    action_data: BaseAction

    def get_action_class(self):
        ...
        # TODO: add logic to get ActionClass based in action_type and naming + inheritance


@dataclasses.dataclass
class EmailAction(BaseAction):
    """Used as part of the schedule configuration"""
    to_email: str
    subject: str
    body: str

    def __format__(self, format_spec):
        s = shorten
        return f'{s(self.to_email)}, {s(self.subject)}'

    def run(self):
        logger.info(f'Sending email to {self.to_email}')

    @property
    def action_type(self) -> ActionType:
        return ActionType.Email


@dataclasses.dataclass
class CacheAction(BaseAction):

    def __format__(self, format_spec):
        return ''

    def run(self):
        logger.info('Execute caching')

    @property
    def action_type(self) -> ActionType:
        return ActionType.Cache


class MyScheduler(AsyncService):
    _instance = None

    def __init__(
        self,
        initials: List[Tuple[str, str, Dict]] | None = None,
        schedule_queue: Optional[asyncio.Queue] = None,
        loop: AbstractEventLoop = None
    ) -> None:
        self.loop = loop
        self.scheduler = AsyncIOScheduler()

        if schedule_queue is None:
            self.queue = schedule_queue
        else:
            self.queue = asyncio.Queue()
        self.initials = initials
        if initials:
            self.add_initials(self.initials)

    def __new__(cls, *args, **kwargs):
        """Singleton pattern"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

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

    def remove_current_scheduled(self):
        logger.debug(f'Removing existing jobs')
        for job in self.scheduler.get_jobs():
            job.remove()

    def refresh_scheduled_jobs(self):
        self.remove_current_scheduled()
        config = load_scheduler_config()
        for config_params in config:
            config_item = ScheduleConfig(config_params)
            okay, error_msg = self.verify_job(config_item.schedule_params)
            if not okay:
                logger.warning(f'Schedule {config_item} bad due to {error_msg}')
                continue
            ...
        # TODO: call method of ActionType to get method to be scheduled


def scheduler_config_file() -> Path:
    c = get_cache_dir()
    return Path(f'{c}/scheduler.json')


def get_scheduler_config() -> list[dict]:
    f = scheduler_config_file()
    if f.exists():
        return json.loads(f.read_text())


def save_scheduler_config(schedule_config: ScheduleConfig):
    config = []
    if current_config := get_scheduler_config():
        config = current_config
    config.append(dataclasses.asdict(schedule_config))
    c = scheduler_config_file()
    c.write_text(json.dumps(config, indent=4))
    logger.info(f'saving configuration {c.as_posix()}')


def load_scheduler_config() -> list[ScheduleConfig]:
    c = scheduler_config_file()
    j = c.read_text()
    return [ScheduleConfig(**config_item) for config_item in json.loads(j)]
