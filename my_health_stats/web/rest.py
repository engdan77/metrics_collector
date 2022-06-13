from fastapi_utils.enums import StrEnum
from loguru import logger
from makefun import create_function
from fastapi import APIRouter
from my_health_stats.orchestrator.generic import Orchestrator
import datetime
from typing import Union

graph_router = APIRouter()
o = Orchestrator()
DagName = StrEnum('DagName', o.get_dag_names())


def graph(**args):
    logger.debug(args)
    return {'foo': 'bar'}


# Some magic to dynamically create API endpoints
for dag_name in o.get_dag_names():
    dag_args = o.get_extract_parameters().get(dag_name)
    all_args = []
    for cls, args in dag_args.items():  # add args for extract class
        for arg, type_ in args.items():
            all_args.append(f'{arg}: {type_.__name__}')
    graph_names = o.get_graph_names(dag_name)  # get all graph names options
    GraphEnum = StrEnum('GraphEnum', graph_names)
    all_args.append(f'graph: GraphEnum')
    for _ in ('from_date', 'to_date'):
        all_args.append(f'{_}: datetime.date')
    gen_func = create_function(f'{dag_name}({", ".join(all_args)})', graph)
    graph_router.add_api_route(f'/{dag_name}', gen_func)

