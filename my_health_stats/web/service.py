from typing import Iterable

import uvicorn
from loguru import logger
import asyncio
from uvicorn_loguru_integration import run_uvicorn_loguru
from fastapi import FastAPI
from pywebio.platform.fastapi import asgi_app
from my_health_stats.orchestrator.generic import Orchestrator
from my_health_stats.scheduler.base import AsyncService
from my_health_stats.web.rest import graph_router
from my_health_stats.web.ui import main_ui

app = FastAPI()
app.include_router(graph_router, prefix='/graph')


class WebServer:

    async_services_start_method = None
    extra_async_services = None

    def __init__(self, extra_async_services: Iterable[AsyncService] = None):
        print("start web")
        self.__class__.extra_async_services = extra_async_services
        self.mounts()
        self.config = uvicorn.Config(
            app=app, host="0.0.0.0", port=5050, log_level="debug"
        )
        app.add_event_handler("startup", WebServer.startup_events)
        self.server = uvicorn.Server(run_uvicorn_loguru(self.config))
        asyncio.run(self.server.serve())

    @staticmethod
    def startup_events():
        """
        This is essential part for starting services after uvicorn event loop been created
        for those services to hook start within.

        :return:
        """
        for service in WebServer.extra_async_services:
            logger.debug(f"starting {service}")
            service.start()

    def mounts(self):
        ui = asgi_app(main_ui)
        app.mount("/ui", ui)

    def start(self):
        self.server.serve()



if __name__ == "__main__":
    ...
