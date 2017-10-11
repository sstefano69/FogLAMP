# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""Core server module"""

import signal
import asyncio
from aiohttp import web
import subprocess

from foglamp import logger
from foglamp.core import routes
from foglamp.core import middleware
from foglamp.core.scheduler import Scheduler
from foglamp.core.http_server import MultiApp

__author__ = "Praveen Garg, Terris Linenbach"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__)  # logging.Logger


class Server:
    """FOGLamp core server. Starts the FogLAMP scheduler and the FogLAMP REST server."""

    """Class attributes"""
    scheduler = None
    """ foglamp.core.Scheduler """

    @staticmethod
    def _make_app():
        """Creates the REST server

        :rtype: web.Application
        """
        app = web.Application(middlewares=[middleware.error_middleware])
        routes.setup(app)
        return app

    @staticmethod
    def _make_core_app():
        """Creates the Service management REST server Core a.k.s service registry

        :rtype: web.Application
        """
        app = web.Application(middlewares=[middleware.error_middleware])
        routes.core_setup(app)
        return app

    @classmethod
    async def _start_scheduler(cls):
        """Starts the scheduler"""
        cls.scheduler = Scheduler()
        await cls.scheduler.start()

    @classmethod
    def _start_storage(cls):
        print("_start_storage")
        try:
            # fix the directory for storage
            # subprocess.Popen(["./storage"], cwd=r"/home/foglamp/Downloads/store/1010")
            subprocess.call('./storage', cwd=r"/home/foglamp/Downloads/store/1010")
        except Exception as ex:
            # TODO: log this execption, remove print
            print(str(ex))

        #TODO: add shutdown in stop

    @classmethod
    def _start_core(cls):
        print("_start_core")
        # https://aiohttp.readthedocs.io/en/stable/_modules/aiohttp/web.html#run_app
        # web.run_app(cls._make_core_app(), host='0.0.0.0', port=8082)
        ma = MultiApp()
        # TODO: make these dynamic
        core_mgt_port = 8082
        svc_port = 8081
        # add to service instance with core_mgt port and service_port
        ma.configure_app(cls._make_core_app(), port=core_mgt_port)
        ma.configure_app(cls._make_app(), port=svc_port)

        ma.run_all()

    @classmethod
    def start(cls):
        """Starts the server"""

        loop = asyncio.get_event_loop()

        # Register signal handlers
        # Registering SIGTERM creates an error at shutdown. See
        # https://github.com/python/asyncio/issues/396
        for signal_name in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                signal_name,
                lambda: asyncio.ensure_future(cls._stop(loop)))

        cls._start_storage()
        # change the order! it works because storage start and registration takes time
        cls._start_core()
        # start scheduler
        # The scheduler must start first because the REST API interacts with it
        loop.run_until_complete(asyncio.ensure_future(cls._start_scheduler()))

    @classmethod
    async def _stop(cls, loop):
        """Attempts to stop the server

        If the scheduler stops successfully, the event loop is
        stopped.
        """
        if cls.scheduler:
            try:
                await cls.scheduler.stop()
                cls.scheduler = None
            except TimeoutError:
                _LOGGER.exception('Unable to stop the scheduler')
                return

        # Cancel asyncio tasks
        for task in asyncio.Task.all_tasks():
            task.cancel()

        loop.stop()
