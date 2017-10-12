# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""Core server module"""
import os
import asyncio
import sys
import time

import aiohttp
import requests
import subprocess
from aiohttp import web

from foglamp import logger
from foglamp.core import routes
from foglamp.core import routes_core
from foglamp.core import middleware
from foglamp.core.scheduler import Scheduler
from foglamp.core.service_registry import service_registry

__author__ = "Praveen Garg, Terris Linenbach, Amarendra K Sinha"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__)  # logging.Logger

_FOGLAMP_ROOT = os.getenv('FOGLAMP_ROOT', '/home/a/Development/FogLAMP')
_STORAGE_PATH = os.path.expanduser(_FOGLAMP_ROOT+'/services/storage/')

_STORAGE_SERVICE_NAME = 'Storage Services'

_WAIT_STOP_SECONDS = 5
"""How many seconds to wait for the core server process to stop"""
_MAX_STOP_RETRY = 5
"""How many times to send TERM signal to core server process when stopping"""


class Server:
    """FOGLamp core server. Starts the FogLAMP scheduler and the FogLAMP REST server."""

    """Class attributes"""
    scheduler = None
    """ foglamp.core.Scheduler """

    logging_configured = False
    """Set to true when it's safe to use logging"""

    _MANAGEMENT_API_PORT = None
    _STORAGE_PORT = 8080
    _STORAGE_MANAGEMENT_PORT = 1081
    _RESTAPI_PORT = 8082

    @classmethod
    def _configure_logging(cls):
        """Alters the root logger to send messages to syslog
           with a filter of WARNING
        """
        if cls.logging_configured:
            return

        logger.setup()
        cls.logging_configured = True

    # TODO: Fix this
    @classmethod
    async def _stop_management_api(cls, app):
        """Stops Management APIStorage if it is running"""
        stopped = False
        loop = asyncio.get_event_loop()
        try:
            url = 'http://localhost:{}'.format(cls._MANAGEMENT_API_PORT) + '/foglamp/service'
            async with aiohttp.ClientSession(loop=loop) as client:
                async with client.get(url) as resp:
                    assert resp.status == 200
                    retval = dict(resp.json())
                    print(retval)
                    svc = retval["services"]
                    for s in svc:
                        # Kill Services first, excluding Storage which will be killed afterwards
                        if _STORAGE_SERVICE_NAME != s["name"]:
                            service_base_url = "{}://{}:{}/".format(s["protocol"], s["address"], s["management_port"])
                            service_shutdown_url = service_base_url+'/shutdown'
                            retval = service_registry.check_shutdown(service_shutdown_url)
        except (OSError, RuntimeError):
            stopped = True

        if not stopped:
            raise TimeoutError("Unable to stop Management API")

        print("Management API stopped")

    @classmethod
    async def _start_storage(cls, app):
        # Start Storage Service
        print("Starting Storage Services")

        try:
            with subprocess.Popen([_STORAGE_PATH + 'storage', '--port={}'.format(cls._MANAGEMENT_API_PORT),
                                   '--address=localhost'], cwd=_STORAGE_PATH) as proc:
                pass
        except OSError as e:
            raise Exception("[{}] {} {} {}".format(e.errno, e.strerror, e.filename, e.filename2))

        # Before proceeding further, do a healthcheck for Storage Services
        try:
            time_left = 10  # 10 seconds enough?
            while time_left:
                time.sleep(1)
                try:
                    _STORAGE_PING_URL = "http://localhost:{}".format(cls._STORAGE_MANAGEMENT_PORT)
                    retval = service_registry.check_service_availibility(_STORAGE_PING_URL)
                    break
                except RuntimeError as e:
                    # Let us try again
                    pass
                time_left -= 1

            if not time_left:
                raise RuntimeError("Unable to start Storage Services")
        except RuntimeError as e:
            raise Exception(str(e))

    # TODO: Fix below
    @classmethod
    async def _stop_storage(cls, app):
        """Stops Storage"""
        stopped = False
        loop = asyncio.get_event_loop()

        # TODO: Fix below. Stopping storage not working from code.
        try:
            url = 'http://localhost:{}'.format(cls._MANAGEMENT_API_PORT) + '/foglamp/service?name=' + _STORAGE_SERVICE_NAME
            async with aiohttp.ClientSession(loop=loop) as client:
                async with client.get(url) as resp:
                    assert resp.status == 200
                    # TODO: Fix below 3 lines when Storage self registers itself
                    # s = dict(resp.json())
                    # _STORAGE_SHUTDOWN_URL = "{}://{}:{}".format(s["protocol"], s["address"], s["management_port"])
                    _STORAGE_SHUTDOWN_URL = "http://localhost:{}".format(cls._STORAGE_MANAGEMENT_PORT)
                    retval = service_registry.check_shutdown(_STORAGE_SHUTDOWN_URL)
        except Exception as err:
            stopped = True

        if not stopped:
            raise TimeoutError("Unable to stop Storage")

        print("Storage stopped")

    @classmethod
    async def _start_scheduler(cls, app):
        """Starts the scheduler"""
        cls.scheduler = Scheduler(cls._MANAGEMENT_API_PORT)
        await cls.scheduler.start()

    @classmethod
    async def _stop_scheduler(cls, app):
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

        print("Foglamp stopped")

    # TODO: fix below two methods
    @classmethod
    async def stop_management_services(cls, core):
        await core.loop.create_task(cls._stop_management_api(core))

    @classmethod
    async def stop_storage_services(cls, core):
        await core.loop.create_task(cls._stop_storage(core))

    @classmethod
    def _make_management(cls):
        """Creates the REST server

        :rtype: web.Application
        """
        core = web.Application(middlewares=[middleware.error_middleware])
        routes_core.setup(core)
        core.on_startup.append(cls._start_storage)
        core.on_startup.append(cls._start_scheduler)
        core.on_shutdown.append(cls._stop_scheduler)
        # TODO: Fix below two methods
        # core.on_shutdown.append(cls._stop_management_api)
        # core.on_shutdown.append(cls.stop_storage_services)
        return core

    @classmethod
    def _make_app(cls):
        """Creates the REST server

        :rtype: web.Application
        """
        app = web.Application(middlewares=[middleware.error_middleware])
        routes.setup(app)
        return app

    @classmethod
    def start(cls):
        cls._configure_logging()
        print("Starting Management API")
        try:
            loop = asyncio.get_event_loop()

            # Management API first
            core = cls._make_management()
            handler1 = core.make_handler()
            coroutine1 = loop.create_server(handler1, '0.0.0.0', 0)
            server1 = loop.run_until_complete(coroutine1)
            address1, cls._MANAGEMENT_API_PORT = server1.sockets[0].getsockname()
            print('Management API started on http://{}:{}'.format(address1, cls._MANAGEMENT_API_PORT))

            loop.run_until_complete(core.startup())

            # Rest Server
            app = cls._make_app()
            handler2 = app.make_handler()
            coroutine2 = loop.create_server(handler2, '0.0.0.0', cls._RESTAPI_PORT)
            server2 = loop.run_until_complete(coroutine2)
            address2, port2 = server2.sockets[0].getsockname()
            print('Rest Server started on http://{}:{}'.format(address2, port2))

            try:
                loop.run_forever()
            except KeyboardInterrupt:
                pass
            finally:
                # TODO: Remove unnecessary calls
                # Important to note the order
                loop.run_until_complete(core.shutdown())
                # loop.run_until_complete(server1.wait_closed())
                # loop.run_until_complete(handler1.shutdown(60.0))
                # loop.run_until_complete(handler1.finish_connections(1.0))
                # loop.run_until_complete(core.cleanup())

                # loop.run_until_complete(app.shutdown())
                # loop.run_until_complete(server2.wait_closed())
                # loop.run_until_complete(handler2.shutdown(60.0))
                # loop.run_until_complete(handler2.finish_connections(1.0))
                # loop.run_until_complete(app.cleanup())

            loop.close()
        except (OSError, RuntimeError, TimeoutError) as e:
            sys.stderr.write('Error: ' + format(str(e)) + "\n")
            sys.exit(1)
        except Exception as e:
            sys.stderr.write('Error: ' + format(str(e)) + "\n")
            sys.exit(1)
