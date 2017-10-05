# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

# FIXME:
""" Template for 'poll' type plugin """

from datetime import datetime, timezone

import random
import uuid

import foglamp.device.exceptions as exceptions

from foglamp import logger

__author__ = "Stefano Simonelli"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__)

# FIXME:
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


def plugin_info():
    """ Template for the plugin_info function for the 'poll' type plugin """

    return {
            'name': 'Poll plugin',
            'version': '1.0',
            'mode': 'poll', ''
            'type': 'device',
            'interface': '1.0',
            'config': _DEFAULT_CONFIG
    }


# noinspection PyUnusedLocal
def plugin_init(config):
    """ Template for the plugin_init function for the 'poll' type plugin """
    
    return {}


# noinspection PyUnusedLocal
async def plugin_poll(handler):
    """ Template for the plugin_poll function for the 'poll' type plugin """

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


def plugin_run():
    """ Template for the plugin_run function for the 'poll' type plugin """
    pass


def plugin_reconfigure(config):
    """ Template for the plugin_reconfigure function for the 'poll' type plugin """

    return config


def plugin_shutdown(data):
    """ Template for the plugin_shutdown function for the 'poll' type plugin """

    return data
