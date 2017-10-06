# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" Template module for 'poll' type plugin """

from datetime import datetime, timezone

import random
import uuid

import foglamp.device.exceptions as exceptions

from foglamp import logger

__author__ = "Stefano Simonelli"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_DEFAULT_CONFIG = {
    'plugin': {
         'description': 'Python module name of the plugin to load',
         'type': 'string',
         'default': 'poll'
    },
    'pollInterval': {
        'description': 'The interval between poll calls to the device poll routine expressed in milliseconds.',
        'type': 'integer',
        'default': '500'
    }

}

_LOGGER = logger.setup(__name__)


def plugin_info():
    """
        Returns information about the plugin.
        plugin_info template for the 'poll' type plugin
    """

    return {
            'name': 'Poll plugin',
            'version': '1.0',
            'mode': 'poll', ''
            'type': 'device',
            'interface': '1.0',
            'config': _DEFAULT_CONFIG
    }


def plugin_init(config):
    """
        Initialise the plugin.
        plugin_init template for the 'poll' type plugin
    """

    handler = {}

    return handler


async def plugin_poll(handler):
    """ plugin_poll template for the 'poll' type plugin """

    time_stamp = datetime.now(tz=timezone.utc)

    try:
        data = {
                'asset':     'poll_template',
                'timestamp': time_stamp,
                'key':       str(uuid.uuid4()),
                'readings':  {'value': random.randint(0, 1000)}
        }

    except Exception as ex:
        raise exceptions.DataRetrievalError(ex)

    return data


def plugin_reconfigure(handler, config):
    """
        Called when the configuration of the plugin is changed during the operation of the device service.
        A new configuration category will be passed.
        plugin_reconfigure template for the 'poll' type plugin
    """

    new_handler = {}

    return new_handler


def plugin_shutdown(handler):
    """
        Called prior to the device service being shut down.
        plugin_shutdown template for the 'poll' type plugin
    """
