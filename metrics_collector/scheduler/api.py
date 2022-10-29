from __future__ import annotations
import dataclasses
import json
import tempfile
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Annotated, Protocol, Type

import apprise
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
from asyncio.events import AbstractEventLoop
import sys
import warnings
from loguru import logger

# Used to overcome "found in sys.modules after import of package .."
from metrics_collector.exceptions import MetricsBaseException
from metrics_collector.helper import import_item
from metrics_collector.orchestrator.generic import Orchestrator
from metrics_collector.utils import shorten, get_data_dir

if not sys.warnoptions:  # allow overriding with `-W` option
    warnings.filterwarnings("ignore", category=RuntimeWarning, module="runpy")


class ActionType(str, Enum):
    Email = "Email"
    Cache = "Cache"


@dataclasses.dataclass
class BaseScheduleParams(ABC):
    """Baseclass for scheduling parameters"""

    def __format__(self, format_spec):
        return str(self.__dict__)


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


@dataclasses.dataclass(kw_only=True)
class BaseAction(ABC):
    """Used for scheduling report to e.g. know whether to email to cache, add required fields to abstract classes"""

    def __repr__(self):
        return str(self.__dict__)

    @staticmethod
    def get_graphs(
        c: ScheduleConfig,
        format_: Annotated[str, "Type such as `html` or `png`"] = "png",
    ) -> list:
        """Used by concrete Action classes when run"""
        output_graphs = []
        o = Orchestrator()
        dag_name, from_, to_ = (c.dag_name, c.from_, c.to_)
        extract_params = o.get_stored_params(c.dag_name)
        extract_objects = o.get_extract_objects(c.dag_name, extract_params)
        o.process_dates(extract_objects, from_, to_)
        transform_object = o.get_transform_object(dag_name, extract_objects)
        for graph_data in o.get_all_graphs(
            from_, to_, dag_name, transform_object, format_
        ):
            output_graphs.append(graph_data)
        return output_graphs

    @abstractmethod
    def run(self, schedule_config: ScheduleConfig):
        """Implement logic for executing this action"""
        ...

    @classmethod
    def action_type(cls) -> ActionType:
        """Return what of what type this action is"""
        ...


@dataclasses.dataclass
class EmailAction(BaseAction):
    """Used as part of the schedule configuration.
    More details found here https://github.com/caronc/apprise/wiki/Notify_email
    """

    to_email: str
    subject: str
    body: str
    mail_server_user: str
    mail_server_password: str

    def __format__(self, format_spec):
        s = shorten
        return f"{s(self.to_email)}, {s(self.subject)}"

    def run(self, schedule_config: ScheduleConfig):
        ext = "png"
        graphs = self.get_graphs(schedule_config, ext)
        send_obj = apprise.Apprise()
        try:
            user, domain = self.mail_server_user.split("@")
        except ValueError:
            logger.error(
                "Mail server user need @domain to determine service - e.g. foo@gmail.com"
            )
            return
        send_obj.add(
            f"mailto://{user}:{self.mail_server_password}@{domain}?to={self.to_email}"
        )
        attach = []
        with tempfile.TemporaryDirectory() as td:
            for graph in graphs:
                f = Path(td) / f"{uuid.uuid4()}.{ext}"
                f.write_bytes(graph)
                attach.append(f.as_posix())
            send_obj.notify(
                title=self.subject,
                body=self.body,
                attach=attach,
            )

    @classmethod
    def action_type(cls) -> ActionType:
        return ActionType.Email


@dataclasses.dataclass
class CacheAction(BaseAction):
    """Main purpose for purely running and do not use graphs for caching purposes"""

    def __format__(self, format_spec):
        return ""

    def run(self, schedule_config: ScheduleConfig):
        logger.info(f"Execute caching of {schedule_config}")
        self.get_graphs(schedule_config, "png")

    @classmethod
    def action_type(cls) -> ActionType:
        return ActionType.Cache


class MyScheduler:
    _instance = None

    def __init__(
        self,
        initials: List[Tuple[str, str, Dict]] | None = None,
        schedule_queue: Optional[asyncio.Queue] = None,
        loop: AbstractEventLoop = None,
    ) -> None:
        self.reload_id = "reload_on_config_update"
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
        self.scheduler.add_job(
            self.reload_on_config_update, "interval", seconds=10, id=self.reload_id
        )
        if self.loop:
            self.loop.create_task(self.start_async())
        else:
            self.scheduler.start()

    @classmethod
    def verify_job(
        cls, schedule_params: ScheduleParams, temp_scheduler=AsyncIOScheduler
    ) -> tuple[Annotated[bool, "success"], Annotated[str, "message"] | None]:
        """Method for verifying schedule params"""
        try:
            job = temp_scheduler().add_job(
                lambda: None, "cron", **schedule_params.asdict()
            )
        except ValueError as e:
            logger.warning(f"wrong schedule param {e}")
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
        logger.debug(f"Removing existing jobs")
        for job in self.scheduler.get_jobs():
            if job.id == self.reload_id:
                continue
            job.remove()

    def refresh_scheduled_jobs(self, trigger_type="interval"):
        self.remove_current_scheduled()
        for config_params in self.current_config:
            okay, error_msg = self.verify_job(config_params.schedule_params)
            if not okay:
                logger.warning(f"Schedule {config_params} bad due to {error_msg}")
                continue
            ...
            logger.debug(
                f"Adding schedule for {config_params.action_type} on {config_params.schedule_params}"
            )
            self.add_task(
                config_params.action_data.run,
                "cron",
                (config_params,),
                **config_params.schedule_params.asdict(),
            )
            ...
        # TODO: call method of ActionType to get method to be scheduled

    def stop(self):
        self._running = False

    async def reload_on_config_update(self):
        new_config = load_scheduler_config()
        if not new_config == self.current_config:
            logger.info("detected new config and reloading")
            self.refresh_scheduled_jobs()
        self.current_config = new_config


def scheduler_config_file() -> Path:
    c = get_data_dir()
    f = Path(f"{c}/scheduler.json")
    if not f.exists():
        f.touch(exist_ok=True)
    return f


def get_scheduler_config() -> list[dict]:
    f = scheduler_config_file()
    if f.exists():
        d = f.read_text()
        return [] if not d else json.loads(d)


def save_scheduler_config(schedule_config: ScheduleConfig):
    config = []
    if current_config := get_scheduler_config():
        config = current_config
    config.append(dataclasses.asdict(schedule_config))
    c = scheduler_config_file()
    c.write_text(json.dumps(config, indent=4))
    logger.info(f"saving configuration {c.as_posix()}")


def load_scheduler_config() -> list[ScheduleConfig]:
    c = scheduler_config_file()
    j = c.read_text()
    return (
        []
        if not j
        else [ScheduleConfig(**config_item) for config_item in json.loads(j)]
    )


class AsyncService(Protocol):
    def start(self):
        ...
