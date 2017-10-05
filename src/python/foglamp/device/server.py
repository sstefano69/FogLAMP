# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""FogLAMP device server"""

import asyncio
import signal
import time

import foglamp.device.exceptions as exceptions
from foglamp import configuration_manager
from foglamp import logger
from foglamp.device.ingest import Ingest


__author__ = "Terris Linenbach"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_DEFAULT_CONFIG = {
    'plugin': {
        'description': 'Python module name of the plugin to load',
        'type': 'string',
        'default': 'CoAP'
    }
}

_LOGGER = logger.setup(__name__)

_PLUGIN_MODULE_PATH = "foglamp.device."
_PLUGIN_MODULE_SUFFIX = "_device"

_MESSAGES_LIST = {

    # Information messages
    "i000000": "",

    # Warning / Error messages
    "e000000": "generic error.",
    "e000001": "cannot proceed the execution, only the type -device- is allowed - plugin name |{0}| plugin type |{1}|",
}
""" Messages used for Information, Warning and Error notice """


class Server:
    """" Implements the Device Service functionalities """

    _plugin_name = None  # type:str
    """"The name of the plugin"""
    
    _plugin = None
    """The plugin's module'"""

    _plugin_data = None
    """The value that is returned by the plugin_init"""

    @classmethod
    async def _stop(cls, loop):
        if cls._plugin is not None:
            try:
                cls._plugin.plugin_shutdown(cls._plugin_data)
            except Exception:
                _LOGGER.exception("Unable to shut down plugin '{}'".format(cls._plugin_name))
            finally:
                cls._plugin = None
                cls._plugin_data = None

        try:
            await Ingest.stop()
        except Exception:
            _LOGGER.exception('Unable to stop the Ingest server')
            return

        # Stop all pending asyncio tasks
        for task in asyncio.Task.all_tasks():
            task.cancel()

        loop.stop()

    @classmethod
    async def _start(cls, plugin: str, loop)->None:
        error = None
        cls.plugin_name = plugin

        try:
            # TODO: Category name column is allows only 10 characters.
            # This needs to be increased

            # Configuration handling - initial configuration
            category = plugin

            config = _DEFAULT_CONFIG
            config['plugin']['default'] = plugin.lower()

            await configuration_manager.create_category(category, config,
                                                        '{} Device'.format(plugin), True)

            config = await configuration_manager.get_category_all_items(category)

            plugin_module = '{path}{name}{suffix}'.format(
                                                            path=_PLUGIN_MODULE_PATH,
                                                            name=config['plugin']['value'],
                                                            suffix=_PLUGIN_MODULE_SUFFIX)

            try:
                cls._plugin = __import__(plugin_module, fromlist=[''])
            except Exception:
                error = 'Unable to load module {} for device plugin {}'.format(plugin_module,
                                                                               plugin)
                raise

            # Plugin initialization
            plugin_info = cls._plugin.plugin_info()
            default_config = plugin_info['config']

            # Configuration handling - updates configuration using information specific to the plugin
            await configuration_manager.create_category(category, default_config,
                                                        '{} Device'.format(plugin))

            config = await configuration_manager.get_category_all_items(category)

            # TODO: Register for config changes

            # Ensures the plugin type is the correct one - 'device'
            if plugin_info['type'] != 'device':

                message = _MESSAGES_LIST['e000001'].format(plugin, plugin_info['type'])
                _LOGGER.error(message)

                raise exceptions.InvalidPluginTypeError()

            cls._plugin_data = cls._plugin.plugin_init(config)

            # Executes the requested plugin type
            if plugin_info['mode'] == 'async':
                await  cls._exec_plugin_async(config)

            elif plugin_info['mode'] == 'poll':
                asyncio.ensure_future(cls._exec_plugin_poll(config))
                asyncio.ensure_future(cls._exec_maintenance(config))

        except Exception:
            if error is None:
                error = 'Failed to initialize plugin {}'.format(plugin)
            _LOGGER.exception(error)
            print(error)
            asyncio.ensure_future(cls._stop(loop))

    @classmethod
    async def _exec_plugin_async(cls, config) -> None:
        """ Executes async type plugin  """

        cls._plugin.plugin_run(cls._plugin_data)

        await Ingest.start()

    @classmethod
    async def _exec_plugin_poll(cls, config) -> None:
        """ Executes poll type plugin """

        await Ingest.start()

        while True:
            data = await cls._plugin.plugin_poll(cls._plugin_data)

            await Ingest.add_readings(asset=data['asset'],
                                      timestamp=data['timestamp'],
                                      key=data['key'],
                                      readings=data['readings'])

            # pollInterval is expressed in milliseconds
            sleep_seconds = int(config['pollInterval']['value']) / 1000.0
            await asyncio.sleep(sleep_seconds)

    @classmethod
    async def _exec_maintenance(cls, config) -> None:
        """ _exec_maintenance

        .. todo::
            * to be implemented
        """

        while True:
            _LOGGER.debug("_exec_maintenance")
            await asyncio.sleep(5)

    @classmethod
    def start(cls, plugin):
        """Starts the device server

        Args:
            plugin: Specifies which device plugin to start
        """
        loop = asyncio.get_event_loop()

        # Register signal handlers
        # Registering SIGTERM causes an error at shutdown. See
        # https://github.com/python/asyncio/issues/396
        for signal_name in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                signal_name,
                lambda: asyncio.ensure_future(cls._stop(loop)))

        asyncio.ensure_future(cls._start(plugin, loop))
        loop.run_forever()

