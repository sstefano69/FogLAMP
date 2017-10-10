# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""Core server module"""
import os
import signal
import asyncio
import setproctitle
import sys
import time
import requests
import subprocess
import socket
from aiohttp import web
from multiprocessing import Process

from foglamp import logger
from foglamp.core import routes
from foglamp.core import routes_core
from foglamp.core import middleware
from foglamp.core.scheduler import Scheduler
from foglamp.core.service_registry import service_registry, instance

__author__ = "Praveen Garg, Terris Linenbach, Amarendra K Sinha"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__)  # logging.Logger

_FOGLAMP_ROOT = os.getenv('FOGLAMP_ROOT', '/home/asinha/Development/FogLAMP')
_STORAGE_PATH =  os.path.expanduser(_FOGLAMP_ROOT+'/services/storage')

_MANAGEMENT_PID_PATH = os.getenv('MANAGEMENT_PID_PATH', os.path.expanduser('~/var/run/management.pid'))
_STORAGE_PID_PATH =  os.getenv('STORAGE_PID_PATH', os.path.expanduser('~/var/run/storage.pid'))
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

    # TODO: Discuss if this needs to be pre-decided and fixed
    _MANAGEMENT_API_PORT = 8081

    # TODO: Set below storage ports to None after Storage layer is fixed to accept these values via command line arguments
    _STORAGE_PORT = 8080
    _STORAGE_MANAGEMENT_PORT = 1081

    # TODO: Fix a TODO in _start() below to accept below via discovery
    _RESTAPI_PORT = 8082

    @staticmethod
    def request_available_port():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('localhost', 0))
        addr, port = s.getsockname()
        s.close()
        return port

    @staticmethod
    def _safe_make_dirs(path):
        """Creates any missing parent directories

        :param path: The path of the directory to create
        """

        try:
            os.makedirs(path, 0o750)
        except OSError as exception:
            if not os.path.exists(path):
                raise exception

    @classmethod
    def _configure_logging(cls):
        """Alters the root logger to send messages to syslog
           with a filter of WARNING
        """
        if cls.logging_configured:
            return

        logger.setup()
        cls.logging_configured = True



    """ Management API """
    @staticmethod
    def _make_management():
        """Creates the REST server

        :rtype: web.Application
        """
        core = web.Application(middlewares=[middleware.error_middleware])
        routes_core.setup(core)
        return core

    @classmethod
    def _run_management_api(cls, _MANAGEMENT_PORT):
        web.run_app(cls._make_management(), host='0.0.0.0', port=_MANAGEMENT_PORT)

    @staticmethod
    def _get_management_pid():
        """Returns FogLAMP's process id or None if FogLAMP is not running"""

        try:
            with open(_MANAGEMENT_PID_PATH, 'r') as pid_file:
                pid = int(pid_file.read().strip())
        except (IOError, ValueError):
            return None

        # Delete the pid file if the process isn't alive
        # there is an unavoidable race condition here if another
        # process is stopping or starting the daemon
        try:
            os.kill(pid, 0)
        except OSError:
            os.remove(_MANAGEMENT_PID_PATH)
            pid = None

        return pid

    @classmethod
    def _start_management_service(cls):
        # Start Management API
        print("Starting Management API")
        try:
            cls._safe_make_dirs(os.path.dirname(_MANAGEMENT_PID_PATH))
            setproctitle.setproctitle('management')
            # Process used instead of subprocess as it allows a python method to run in a separate process.

            # TODO: Investigate if below line is required and remove it if _MANAGEMENT_API_PORT is going to be fixed
            # cls._MANAGEMENT_API_PORT = cls.request_available_port()

            m = Process(target=cls._run_management_api, name='core', args=(cls._MANAGEMENT_API_PORT,))
            m.start()

            # Create management pid in ~/var/run/storage.pid
            with open(_MANAGEMENT_PID_PATH, 'w') as pid_file:
                pid_file.write(str(m.pid))
        except OSError as e:
            raise Exception("[{}] {} {} {}".format(e.errno, e.strerror, e.filename, e.filename2))

        # Before proceeding further, do a healthcheck for Management API
        try:
            time_left = 10  # 10 seconds enough?
            _CORE_PING_URL = "http://localhost:{}/foglamp/service/ping".format(cls._MANAGEMENT_API_PORT)
            print(cls._MANAGEMENT_API_PORT, _CORE_PING_URL)
            while time_left:
                time.sleep(1)
                try:
                    retval = service_registry.check_service_availibility(_CORE_PING_URL)
                    break
                except RuntimeError as e:
                    # Let us try again
                    pass
                time_left -= 1
            if not time_left:
                raise RuntimeError("Unable to start Management API")
        except RuntimeError as e:
            raise Exception(str(e))

    @classmethod
    def _stop_management_service(cls, pid=None):
        """Stops Storage if it is running

        Args:
            pid: Optional process id to stop. If not specified, the pidfile is read.

        Raises TimeoutError:
            Unable to stop Storage. Wait and try again.
        """
        if not pid:
            pid = cls._get_management_pid()

        if not pid:
            print("Management API is not running")
            return

        stopped = False

        try:
            l = requests.get('http://localhost:{}'.format(cls._MANAGEMENT_API_PORT) + '/foglamp/service')
            assert 200 == l.status_code

            retval = dict(l.json())
            svc = retval["services"]
            for s in svc:
                # Kill Services first, excluding Storage which will be killed afterwards
                if _STORAGE_SERVICE_NAME != s["name"]:
                    service_base_url = "{}://{}:{}/".format(s["protocol"], s["address"], s["management_port"])
                    service_shutdown_url = service_base_url+'/shutdown'
                    retval = service_registry.check_shutdown(service_shutdown_url)

            # Now kill the Management API
            for _ in range(_MAX_STOP_RETRY):
                os.kill(pid, signal.SIGTERM)

                for _ in range(_WAIT_STOP_SECONDS):  # Ignore the warning
                    os.kill(pid, 0)
                    time.sleep(1)
                    os.remove(_MANAGEMENT_PID_PATH)
        except (OSError, RuntimeError):
            stopped = True

        if not stopped:
            raise TimeoutError("Unable to stop Management API")

        print("Management API stopped")



    """ Storage Services """
    @staticmethod
    def _get_storage_pid():
        """Returns Storage's process id or None if Storage is not running"""

        try:
            with open(_STORAGE_PID_PATH, 'r') as pid_file:
                pid = int(pid_file.read().strip())
        except (IOError, ValueError):
            return None

        # Delete the pid file if the process isn't alive
        # there is an unavoidable race condition here if another
        # process is stopping or starting the daemon
        try:
            os.kill(pid, 0)
        except OSError:
            os.remove(_STORAGE_PID_PATH)
            pid = None

        return pid

    @classmethod
    def _start_storage(cls):
        """Starts Storage"""
        cls._safe_make_dirs(os.path.dirname(_STORAGE_PID_PATH))
        pid = cls._get_storage_pid()
        if pid:
            print("Storage is already running in PID {}".format(pid))
        else:
            # TODO: Uncomment below after Storage layer is fixed to accept below via command line params
            # cls._STORAGE_PORT = cls.request_available_port()
            p = subprocess.Popen([_STORAGE_PATH+'/storage', '--port={}'.format(cls.request_available_port()), '--address=localhost'])

            # Create storage pid in ~/var/run/storage.pid
            with open(_STORAGE_PID_PATH, 'w') as pid_file:
                pid_file.write(str(p.pid))

    @classmethod
    def _start_storage_service(cls):
        # Start Storage Service
        print("Starting Storage Services")
        try:
            setproctitle.setproctitle('storage')
            cls._start_storage()
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

    @classmethod
    def _stop_storage(cls, pid=None):
        """Stops Storage if it is running

        Args:
            pid: Optional process id to stop. If not specified, the pidfile is read.

        Raises TimeoutError:
            Unable to stop Storage. Wait and try again.
        """

        if not pid:
            pid = cls._get_storage_pid()

        if not pid:
            print("Storage is not running")
            return

        stopped = False

        try:
            l = requests.get('http://localhost:{}'.format(cls._MANAGEMENT_API_PORT) + '/foglamp/service?name=' + _STORAGE_SERVICE_NAME)
            assert 200 == l.status_code

            s = dict(l.json())
            # TODO: Fix below 2 lines when Storage self registers itself
            # _STORAGE_SHUTDOWN_URL = "{}://{}:{}".format(s["protocol"], s["address"], s["management_port"])
            _STORAGE_SHUTDOWN_URL = "http://localhost:{}".format(cls._STORAGE_MANAGEMENT_PORT)
            retval = service_registry.check_shutdown(_STORAGE_SHUTDOWN_URL)
        except Exception as err:
            stopped = True

        if not stopped:
            raise TimeoutError("Unable to stop Storage")

        os.remove(_STORAGE_PID_PATH)

        print("Storage stopped")

    @classmethod
    def _status_storage(cls):
        """Outputs the status of the Storage process"""
        pid = cls._get_storage_pid()

        if pid:
            print("Storage is running in PID {}".format(pid))
        else:
            print("Storage is not running")
            sys.exit(2)


    """ Foglamp Server """
    @staticmethod
    def _make_app():
        """Creates the REST server

        :rtype: web.Application
        """
        app = web.Application(middlewares=[middleware.error_middleware])
        routes.setup(app)
        return app

    @classmethod
    async def _start_scheduler(cls):
        """Starts the scheduler"""
        cls.scheduler = Scheduler()
        await cls.scheduler.start()

    @classmethod
    def _start(cls):
        """Starts the server"""
        loop = asyncio.get_event_loop()

        # Register signal handlers
        # Registering SIGTERM creates an error at shutdown. See
        # https://github.com/python/asyncio/issues/396
        for signal_name in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                signal_name,
                lambda: asyncio.ensure_future(cls._stop(loop)))

        # The scheduler must start first because the REST API interacts with it
        loop.run_until_complete(asyncio.ensure_future(cls._start_scheduler()))

        # We need to register before starting the server as the server, once started, will go into loop waiting mode,
        # and if there is any error/exception, the main start try-catch will abort everything.
        # TODO: Fix below
        # instance.Service.Instances.register(cls, "admin", "admin", "localhost", cls._RESTAPI_PORT, cls._RESTAPI_PORT, protocol='http')

        # https://aiohttp.readthedocs.io/en/stable/_modules/aiohttp/web.html#run_app
        web.run_app(cls._make_app(), host='0.0.0.0', port=cls._RESTAPI_PORT, handle_signals=False)

    @classmethod
    def start(cls):
        cls._configure_logging()

        try:
            cls._start_management_service()
            cls._start_storage_service()

            # Everthing ok, so now start Foglamp Server
            print("Starting FogLAMP")
            try:
                setproctitle.setproctitle('foglamp')
                cls._start()
            except OSError as e:
                raise Exception("[{}] {} {} {}".format(e.errno, e.strerror, e.filename, e.filename2))

        except Exception as e:
            sys.stderr.write('Error: '+format(str(e)) + "\n");
            sys.exit(1)

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

        # Stop foglamp
        loop.stop()

        # Stop Storage Service
        cls._stop_storage()

        # Stop Management API
        cls._stop_management_service()
