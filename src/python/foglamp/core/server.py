# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""Core server module"""

import os
import signal
import asyncio
from aiohttp import web
import subprocess

from foglamp import logger
from foglamp.core import routes
from foglamp.core import middleware
from foglamp.core.scheduler import Scheduler
from foglamp.core.http_server import MultiApp
from foglamp.core.service_registry.instance import Service

__author__ = "Praveen Garg, Terris Linenbach"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_logger = logger.setup(__name__, level=20)

# TODO: FIXME: the ROOT directory
_FOGLAMP_ROOT = '/home/foglamp/foglamp/FogLAMP'
_STORAGE_DIR = os.path.expanduser(_FOGLAMP_ROOT + '/services/storage')
_STORAGE_DIR = r"/home/foglamp/Downloads/store/1010"

_API_SERVICE_PORT = 8081
HOST = 'localhost'


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
    async def _start_scheduler(cls, core_mgt_port):
        """Starts the scheduler"""
        print("called _start_scheduler")
        _logger.info("start scheduler")
        cls.scheduler = Scheduler(core_mgt_port)
        await cls.scheduler.start()

    @classmethod
    async def start_storage(cls, loop, host, m_port):
        loop.call_soon(cls._start_storage(host, m_port))

    @staticmethod
    def _start_storage(host, m_port):
        print("called _start_storage")
        _logger.info("start storage")
        try:
            cmd_with_args = ['./storage', '--address={}'.format(host),
                             '--port={}'.format(m_port)]
            subprocess.call(cmd_with_args, cwd=_STORAGE_DIR)
        except Exception as ex:
            _logger.exception(str(ex))

    @classmethod
    def _start_core(cls, host,  service_port, management_port=0):
        print("called _start_core")
        _logger.info("start core")

        ma = MultiApp()
        ma.configure_app(cls._make_core_app(), host=host, port=management_port)
        ma.configure_app(cls._make_app(), host=host, port=service_port)
        # TODO: allow config / env var to set protocol
        cls._register_core(host, management_port, service_port)
        ma.run_all()

    @classmethod
    def _register_core(cls, host, mgt_port, service_port):
        core_service_id = Service.Instances.register(name="FogLAMP Core", s_type="Core", address=host,
                                                     port=service_port, management_port=mgt_port)

        return core_service_id

    # TODO: remove me | NOT NEEDED (hopefully, we shall be able to get info back from aiohttp)
    @classmethod
    def request_available_port(cls, host='localhost'):
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, 0))
        addr, port = s.getsockname()
        # closed ?!
        s.close()
        return port

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

        host_address = HOST  # fix?!

        # as of now, can't bind to 0 as we need to know
        # a) to pass this to stoarge and scheduler
        # b) and to register the 1 service for both aiohttp instances
        # however, there for b) these is a way to register see TODO in foglamp.core.http_server.MultiApp#run_all

        _core_mgt_port = cls.request_available_port()

        # start storage after core; which starts the loop
        loop.create_task(cls.start_storage(loop, host_address, _core_mgt_port))

        # start scheduler after storage
        # start the scheduler, but self._ready flag will be enabled
        # only when storage service ping status is up,
        # so that it can read storage to read scheduled_processes and schedules
        loop.create_task(cls._start_scheduler(_core_mgt_port))

        cls._start_core(host=host_address, service_port=_API_SERVICE_PORT, management_port=_core_mgt_port)
        #
        # see http://0.0.0.0:<core_mgt_port>/foglamp/service for registered services

    @staticmethod
    def stop_storage():
        # TODO: make client call to service mgt API to ask to shutdown storage
        try:
            from foglamp.storage.storage import Storage
            Storage().shutdown()
        except Exception as ex:
            _logger.exception(str(ex))

    @classmethod
    async def _stop_core(cls):
        # wait for stop storage to unregister?!
        # does not matter btw! as in memory service_registry is in memory
        # stop aiohttp (shutdown apps)
        pass

    # I doubt! this has ever been called?
    # rename to stop scheduler?!
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
                _logger.exception('Unable to stop the scheduler')
                return

        # Cancel asyncio tasks
        for task in asyncio.Task.all_tasks():
            task.cancel()

        loop.stop()
