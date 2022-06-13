import datetime
from typing import Iterable, Type, Annotated

import pywebio.input
from pywebio.input import input, FLOAT, radio
from pywebio.output import put_html

from my_health_stats.extract.base import BaseExtractParameters, BaseExtract
from my_health_stats.load.base import BaseLoadGraph
from my_health_stats.orchestrator.generic import Orchestrator, ClassType
from loguru import logger

from my_health_stats.transform import BaseTransform

Params = dict[Annotated[str, "param name"], Annotated[str, "value"]]

def space_shifter(text: str) -> str:
    if '_' in text:
        return text.replace('_', ' ').capitalize()
    else:
        return text.replace(' ', '_').lower()


def ui_get_params(param_defintion: dict[Annotated[str, "parm name"], Type]) -> Params:
    """Take dict and get data using Web UI"""
    args = {}
    for label, type_ in param_defintion.items():
        match type_.__name__:
            case 'str' | 'int':
                args[label] = pywebio.input.input(label)
    return args


def dict_to_extract_params_object(params: Params, extract_class: BaseExtract) -> BaseExtractParameters:
    cls = extract_class.get_extract_parameter_class()
    obj = cls(**params)
    return obj


def main_ui():
    o = Orchestrator()

    # get which dag
    args = o.get_extract_parameters()
    logger.debug(args)
    dag_name = space_shifter(radio("Chose what to extract", options=[space_shifter(str(arg)) for arg in args.keys()]))
    logger.debug(f'{dag_name=}')

    # create extract objects
    extract_classes = o.get_registered_classes(dag_name, ClassType.extract)
    extract_objects = []
    for extract_class in extract_classes:
        extract_args = args[dag_name][extract_class]
        logger.debug(f"get arguments for {extract_class=} which is {extract_args}")
        a = ui_get_params(extract_args)
        p = dict_to_extract_params_object(a, extract_class)
        extract_object = extract_class(p)  # add args
        extract_objects.append(extract_object)

    # create transform object
    transformer_class = o.get_registered_classes(dag_name, ClassType.transform, only_first=True)
    transformer_instance = transformer_class(*extract_objects)

    # create load/graph object
    load_class: Type[BaseLoadGraph] = o.get_registered_classes(dag_name, ClassType.load, only_first=True)
    from_ = input('From date', type='date')
    to_ = input('To date', type='date')
    load_instance = load_class(transformer_instance, from_, to_)
    graph_methods = load_instance.get_all_graph_methods()
    logger.debug(graph_methods)
    for graph in load_instance.get_all_graph_methods():
        put_html(load_instance.to_html(graph))
