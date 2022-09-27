import dataclasses
import json
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Annotated, Type
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
from asyncio.events import AbstractEventLoop
import sys
import warnings
from loguru import logger

# Used to overcome "found in sys.modules after import of package .."
from metrics_collector.helper import import_item
from metrics_collector.scheduler.base import BaseAction, BaseScheduleParams, ActionType
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

    def asdict(self):
        return dataclasses.asdict(self)


@dataclasses.dataclass
class ScheduleConfig:
    dag_name: str
    from_: str
    to_: str
    extract_params: dict
    schedule_params: ScheduleParams | dict
    action_type: ActionType | str  # being Enum or string
    action_data: BaseAction | dict  # being objected of type BaseAction

    def __post_init__(self):
        """Instantiate using dict coming from configuration and set action_data"""
        if not issubclass(self.action_type.__class__, ActionType):
            self.action_type = getattr(ActionType, self.action_type)
        if not issubclass(self.action_data.__class__, BaseAction):
            cls = self.get_action_class()
            self.action_data = cls(**self.action_data)
        if not issubclass(self.schedule_params.__class__, ScheduleParams):
            self.schedule_params = ScheduleParams(**self.schedule_params)

    def get_action_class(self) -> Type[BaseAction]:
        """Get subclass based on Enum of field action_type"""
        for subclass in BaseAction.__subclasses__():
            if subclass.action_type() == self.action_type:
                return subclass
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

    @classmethod
    def action_type(cls) -> ActionType:
        return ActionType.Email


@dataclasses.dataclass
class CacheAction(BaseAction):

    def __format__(self, format_spec):
        return ''

    def run(self):
        logger.info('Execute caching')

    @classmethod
    def action_type(cls) -> ActionType:
        return ActionType.Cache


class MyScheduler:
    _instance = None

    def __init__(
        self,
        initials: List[Tuple[str, str, Dict]] | None = None,
        schedule_queue: Optional[asyncio.Queue] = None,
        loop: AbstractEventLoop = None
    ) -> None:
        self.reload_id = 'reload_on_config_update'
        self.loop = loop
        self.scheduler = AsyncIOScheduler()

        if schedule_queue is None:
            self.queue = schedule_queue
        else:
            self.queue = asyncio.Queue()
        self.initials = initials
        if initials:
            self.add_initials(self.initials)
        self.current_config = load_scheduler_config()
        self.refresh_scheduled_jobs()
        self._running = None

    def __new__(cls, *args, **kwargs):
        """Singleton pattern"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def start(self):
        self._running = True
        self.scheduler.add_job(self.reload_on_config_update, 'interval', seconds=10, id=self.reload_id)
        if self.loop:
            self.loop.create_task(self.start_async())
        else:
            self.scheduler.start()

    @classmethod
    def verify_job(cls, schedule_params: ScheduleParams, temp_scheduler=AsyncIOScheduler) -> tuple[Annotated[bool, "success"], Annotated[str, "message"] | None]:
        """Method for verifying schedule params"""
        try:
            job = temp_scheduler().add_job(lambda: None, 'cron', **schedule_params.asdict())
        except ValueError as e:
            logger.warning(f'wrong schedule param {e}')
            return False, str(e)
        else:
            job.remove()
            return True, None

    async def start_async(self):
        self.scheduler.start()

    def add_task(self, _func, _type, _args=None, _kwargs=None, **kwargs):
        if _kwargs is None:
            _kwargs = {}
        if _args is None:
            _args = []
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
            if job.id == self.reload_id:
                continue
            job.remove()

    def refresh_scheduled_jobs(self, trigger_type='interval'):
        self.remove_current_scheduled()
        for config_params in self.current_config:
            okay, error_msg = self.verify_job(config_params.schedule_params)
            if not okay:
                logger.warning(f'Schedule {config_params} bad due to {error_msg}')
                continue
            ...
            logger.debug(f'Adding schedule for {config_params.action_type} on {config_params.schedule_params}')
            self.add_task(config_params.action_data.run, 'cron', **config_params.schedule_params.asdict())
            ...
        # TODO: call method of ActionType to get method to be scheduled

    def stop(self):
        self._running = False

    async def reload_on_config_update(self):
        new_config = load_scheduler_config()
        if not new_config == self.current_config:
            logger.info('detected new config and reloading')
            self.refresh_scheduled_jobs()
        self.current_config = new_config


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
    # TODO: assure config items get properly instantiated
    return [ScheduleConfig(**config_item) for config_item in json.loads(j)]
