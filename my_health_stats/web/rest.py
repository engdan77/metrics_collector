from fastapi_utils.enums import StrEnum
from loguru import logger
from makefun import create_function
from fastapi import APIRouter
from my_health_stats.orchestrator.generic import Orchestrator

graph_router = APIRouter()
o = Orchestrator()
DagName = StrEnum('DagName', o.get_dag_names())


def graph(**args):
    logger.debug(args)
    return {'foo': 'bar'}


# Some magic to dynamically create API endpoints
for _ in o.get_dag_names():
    dag_args = o.get_extract_parameters().get(_)
    all_args = []
    for cls, args in dag_args.items():
        for arg, type_ in args.items():
            all_args.append(f'{arg}: {type_.__name__}')
    gen_func = create_function(f'{_}({", ".join(all_args)})', graph)
    graph_router.add_api_route(f'/{_}', gen_func)

