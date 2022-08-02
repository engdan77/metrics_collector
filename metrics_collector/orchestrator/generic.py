from __future__ import annotations  # required to avoid circular imports for typing purposes

import asyncio
import datetime
import pickle
import shelve
import zlib
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Type, Annotated, Iterable, Callable, Union, Generator, Any, Protocol
import metrics_collector
from enum import Enum, auto
from typing import TYPE_CHECKING
from loguru import logger
from functools import wraps

from metrics_collector.utils import get_days_between, get_cache_dir

if TYPE_CHECKING:
    from metrics_collector.extract.base import parameter_dict, BaseExtract, BaseExtractParameters  # only when typing
    from metrics_collector.transform.base import BaseTransform
    from metrics_collector.load.base import BaseLoadGraph

Params = dict[Annotated[str, "param name"], Annotated[str, "value"]]


class ClassType(str, Enum):
    extract = auto()
    transform = auto()
    load = auto()


class ProgressBar(ABC):
    @abstractmethod
    def __init__(self):
        """Instantiate a progressbar"""

    @abstractmethod
    def update(self, progress: Annotated[float, "Between 0 and 1"]) -> None:
        """Updates its status"""
        ...


def cache_graph_data(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        cache_dir = get_cache_dir()
        cache_file = f'{cache_dir}/graph_cache'
        today = datetime.date.today()
        signature = f"[{today}, {func.__name__}({args})]"
        s = shelve.open(cache_file)
        existing: bytes | None = s.get(signature, None)
        if existing:
            result = pickle.loads(zlib.decompress(existing))
            s.close()
            logger.debug(f'found cached data {len(result) if result else 0} bytes for {signature}')
            return result
        else:
            result = func(*args, **kwargs)
            compressed = zlib.compress(pickle.dumps(result))
            s[signature] = compressed
            logger.debug(f'caching and compressing from {len(result) if result else 0} bytes -> {len(compressed)} bytes for {signature}')
            s.close()
            return result
    return wrapped


def register_dag_name(cls):
    c = cls.dag_name
    names = (c,) if isinstance(c, str) else c
    for _ in names:
        Orchestrator.registered_etl_entities[_].append(cls)


class Orchestrator:
    """Orchestrator acting as proxy for the ETL processes

    Example:
        o = Orchestrator()  # instantiate orchestrator
        dag_name, from_, to_ = ('garmin_and_apple', '2022-03-01', '2022-03-10')  # get dag_name and period
        extract_params_definition = o.get_extract_params_def(dag_name)  # OPTIONAL: dynamically get required parameters
        extract_params = {'apple_uri_health_data': 'ftp://foo:bar@127.0.0.1:10021/export.zip', 'garmin_username': 'foo', 'garmin_password': 'bar'}  # required extract params based on abstract class registered for dag_name
        extract_params = o.get_stored_params(dag_name)  # OPTIONAL: method allow get previous cached data
        extract_objects = o.get_extract_objects(dag_name, extract_params)  # EXTRACT: required with extract_params as dict
        pb = my_progress_bar()  # OPTIONAL: callback function presenting progress between 0.0 to 1.0
        o.process_dates(extract_objects, from_, to_, progress_bar=pb)  # processing those dates
        transform_object = o.get_transform_object(dag_name, extract_objects)  # TRANSFORM: important to be used next step
        for graph_data in o.get_all_graphs(from_, to_, dag_name, transform_object, 'png'):  # LOAD: used to get graph results
            do_something_with_graph_data(graph_data)  # custom handler for handling e.g. png or html
    """

    registered_etl_entities: defaultdict[
        Annotated[str, "dag name"], list[Annotated[Type, "classes"]]
    ] = defaultdict(list)

    def __init__(self):
        self.type = {ClassType.extract: metrics_collector.extract.base.BaseExtract,
                     ClassType.transform: metrics_collector.transform.base.BaseTransform,
                     ClassType.load: metrics_collector.load.base.BaseLoadGraph}

    def get_dag_names(self) -> list:
        """Get what registered services"""
        return list(self.registered_etl_entities.keys())

    def get_extract_services_and_parameters(self) -> dict[Annotated[str, 'dag_name'], dict[str, parameter_dict]]:
        """Get what registered services and their parameters"""
        result = defaultdict(dict)
        for dag_name in self.registered_etl_entities.keys():
            extract_classes = self._get_registered_classes(dag_name, ClassType.extract)
            for cls in extract_classes:
                result[dag_name][cls] = cls.get_parameters()
        return result

    def get_graph_names(self, dag_name: str) -> list[str]:
        extract_classes = self._get_registered_classes(dag_name, ClassType.load)
        methods = []
        for cls in extract_classes:
            for m in cls.get_all_graph_methods(cls):
                methods.append(m.__name__)
        return methods

    def _get_registered_classes(self, dag_name, class_type: ClassType, only_first=False) -> list[Type[BaseExtract] | Type[BaseTransform] | Type[BaseLoadGraph]] | Type[BaseExtract] | Type[BaseTransform] | Type[BaseLoadGraph]:
        logger.debug(f'starting {__name__=}')
        classes = [cls for cls in self.registered_etl_entities.get(dag_name, []) if cls.__base__ is self.type.get(class_type, None)]
        logger.debug('done get classes')
        result = next(iter(classes), None) if only_first else classes
        return result

    @staticmethod
    def _dict_to_extract_params_object(params: Params, extract_class: BaseExtract) -> BaseExtractParameters:
        cls = extract_class.get_extract_parameter_class()
        declare_params = [k for k, v in cls.__dataclass_fields__.items() if v.init]
        # only pass params that are valid for the class
        obj = cls(**{key: params[key] for key in declare_params})
        return obj

    def get_stored_params(self, dag_name) -> dict:
        """Used to retrieve existing params for usage"""
        r = {}
        extract_classes = self._get_registered_classes(dag_name, ClassType.extract)
        for extract_class in extract_classes:
            r.update(extract_class.get_stored_params().items())  # update r with all existing params
        return r

    def get_extract_params_def(self, dag_name):
        """Used to e.g. pass to UI to request params from use"""
        args = self.get_extract_services_and_parameters()
        extract_classes = self._get_registered_classes(dag_name, ClassType.extract)
        for extract_class in extract_classes:
            yield args[dag_name][extract_class]

    def get_extract_objects(self, dag_name, extract_params: dict):
        """Main entrypoint for getting extract objects used to get transformer object"""
        # create extract objects
        args = self.get_extract_services_and_parameters()
        extract_classes = self._get_registered_classes(dag_name, ClassType.extract)
        extract_objects = []
        for extract_class in extract_classes:
            extract_args = args[dag_name][extract_class]
            logger.debug(f"get arguments for {extract_class=} which is {extract_args}")
            p = self._dict_to_extract_params_object(extract_params, extract_class)
            extract_object = extract_class(p)  # add args
            extract_objects.append(extract_object)
        return extract_objects

    def get_transform_object(self, dag_name: str, extract_objects: list) -> BaseTransform:
        """Main entrypoint for getting transform object used to load graph from"""
        # create transform object
        transformer_class = self._get_registered_classes(dag_name, ClassType.transform, only_first=True)
        transformer_object = transformer_class(*extract_objects)
        return transformer_object

    def get_all_graphs(self, from_: datetime.date | str, to_: datetime.date | str, dag_name: str, transform_object: BaseTransform, format: Annotated[str, "Type such as `html` or `png`"] = 'html') -> Generator[Any, None, None]:
        """Main entrypoint for getting all graph objects with methods such as .to_htm() or .to_png()"""
        load_class: Type[BaseLoadGraph] = self._get_registered_classes(dag_name, ClassType.load, only_first=True)
        load_instance = load_class(transform_object, from_, to_)
        for graph in load_instance.get_all_graph_methods():
            yield getattr(load_instance, f'to_{format}')(graph)

    @cache_graph_data
    def get_graph(self, graph_name: str, from_: datetime.date | str, to_: datetime.date | str, dag_name: str, transform_object: BaseTransform, format_: Annotated[str, "Type such as `html` or `png`"] = 'html') -> Any:
        """Main entrypoint for getting all graph objects with methods such as .to_htm() or .to_png()"""
        load_class: Type[BaseLoadGraph] = self._get_registered_classes(dag_name, ClassType.load, only_first=True)
        logger.debug('done get registered classes')
        logger.debug(f'{load_class=}')
        load_instance = load_class(transform_object, from_, to_)
        logger.debug(f'{load_instance=}')
        for graph in load_instance.get_all_graph_methods():
            logger.debug(f'{graph=}')
            if graph.__name__ == graph_name:
                return getattr(load_instance, f'to_{format_}')(graph)

    @staticmethod
    @cache_graph_data
    def process_dates(extract_objects, from_, to_, progress_bar: ProgressBar | None = None):
        """Method of assure data retrieved from service for the period given"""
        dates = list(get_days_between(from_, to_))
        tot = len(list(dates)) * len(extract_objects)
        for idx_extract, extract_object in enumerate(extract_objects, start=1):
            for idx_date, date in enumerate(dates):
                current_count = idx_date * idx_extract
                logger.info(f'downloading {current_count}/{tot} [{extract_object}]')
                extract_object.get_data(date)  # This could also be changed to different context
                if progress_bar:
                    progress_bar.update(current_count / tot)
