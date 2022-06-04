import datetime
from typing import Iterable, Type, Annotated

import pywebio.input
from pywebio.input import input, FLOAT, radio
from pywebio.output import put_text

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
    args = o.get_extract_parameters()
    logger.debug(args)
    dag_name = space_shifter(radio("Chose what to extract", options=[space_shifter(str(arg)) for arg in args.keys()]))
    logger.debug(f'{dag_name=}')
    extract_classes = o.get_registered_classes(dag_name, ClassType.extract)

    # create extract objects
    extract_objects = []
    for extract_class in extract_classes:
        extract_args = args[dag_name][extract_class]
        logger.debug(f"get arguments for {extract_class=} which is {extract_args}")
        a = ui_get_params(extract_args)
        p = dict_to_extract_params_object(a, extract_class)
        extract_object = extract_class(p)  # add args
        extract_objects.append(extract_object)

    transformer_class: BaseTransform = o.get_registered_classes(dag_name, ClassType.transform, only_first=True)
    t = transformer_class(*extract_objects)
    load_class: BaseLoadGraph = o.get_registered_classes(dag_name, ClassType.load, only_first=True)
    from_ = datetime.date(2022, 1, 1)
    to_ = datetime.date(2022, 1, 3)
    l = load_class(t, from_, to_)
    graphs = o.get_graph_methods(dag_name)
    logger.debug(graphs)

    # height = input("Input your height(cm)：", type=FLOAT)
    # weight = input("Input your weight(kg)：", type=FLOAT)
    #
    # BMI = weight / (height / 100) ** 2
    #
    # top_status = [(16, 'Severely underweight'), (18.5, 'Underweight'),
    #               (25, 'Normal'), (30, 'Overweight'),
    #               (35, 'Moderately obese'), (float('inf'), 'Severely obese')]
    #
    # for top, status in top_status:
    #     if BMI <= top:
    #         put_text('Your BMI: %.1f. Category: %s' % (BMI, status))
    #         break