#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" The sending process is run according to a schedule in order to send reading data to the historian,
e.g. the PI system.
It’s role is to implement the rules as to what needs to be sent and when,
extract the data from the storage subsystem and stream it to the translator for sending to the external system.
The sending process does not implement the protocol used to send the data,
that is devolved to the translation plugin in order to allow for flexibility in the translation process.

.. _send_information::

"""

import asyncio
import sys

from foglamp import logger, configuration_manager

import importlib

# Module information
__author__ = "${FULL_NAME}"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_TRANSLATOR_PATH = "translators."

# Plugin handling - loading an empty plugin
_module_template = _TRANSLATOR_PATH + "empty_translator"
_plugin = importlib.import_module(_module_template)
_plugin_info = ""


# FIXME: it will be removed using the DB layer
_DB_URL = 'postgresql:///foglamp'
"""DB references"""

_module_name = "Sending Process"

_message_list = {

    # Information messages
    "i000001": _module_name + " - Started.",
    "i000002": _module_name + " - Execution completed.",

    # Warning / Error messages
    "e000001": _module_name + " - cannot setup the logger - error details |{0}|",
    "e000002": _module_name + " - cannot complete the operation - error details |{0}|",
    "e000003": _module_name + " - cannot retrieve the configuration.",
    "e000004": _module_name + " - cannot complete the initialization.",
    "e000005": _module_name + " - cannot load the plugin |{0}|",
    "e000006": _module_name + " - invalid input parameters, the stream id is required - parameters |{0}|",
}
"""Messages used for Information, Warning and Error notice"""

_logger = ""

_event_loop = ""

_stream_id = 0

_DEFAULT_OMF_CONFIG = {
    "enable": {
        "description": "A switch that can be used to enable or disable execution of the sending process.",
        "type": "boolean",
        "default": "true"
    },
    "duration": {
        "description": "How long the sending process should run before stopping.",
        "type": "integer",
        "default": "60"
    },
    "source": {
      "description": "Defines the source of the data to be sent on the stream, "
                     "this may be one of either readings, statistics or audit.",
      "type": "string",
      "default": "readings"
    },
    "blockSize": {
      "description": "The size of a block of readings to send in each transmission.",
      "type": "integer",
      "default": "1000"
    },
    "sleepInterval": {
      "description": "A period of time, expressed in seconds, "
                     "to wait between attempts to send readings when there are no readings to be sent.",
      "type": "integer",
      "default": "10"
    },
    "translator": {
      "description": "The name of the translator to use to translate the readings into the output format and send them",
      "type": "string",
      "default": "omf_translator_new"
    },

}
_CONFIG_CATEGORY_NAME = 'SEND_PR'
_CONFIG_CATEGORY_DESCRIPTION = 'Configuration of the Sending Process'

# Configurations retrieved from the Configuration Manager
_config_from_manager = ""
_config = {
    'enable': _DEFAULT_OMF_CONFIG['enable']['default'],
    'duration': _DEFAULT_OMF_CONFIG['duration']['default'],
    'source': _DEFAULT_OMF_CONFIG['source']['default'],
    'blockSize': _DEFAULT_OMF_CONFIG['blockSize']['default'],
    'sleepInterval': _DEFAULT_OMF_CONFIG['sleepInterval']['default'],
    'translator': _DEFAULT_OMF_CONFIG['translator']['default'],
}

# DB operations
_pg_conn = ""
_pg_cur = ""


class InvalidCommandLineParameters(RuntimeError):
    """ InvalidCommandLineParameters """
    pass


def _retrieve_configuration(process_key):
    """ Retrieves the configuration from the Configuration Manager

    Args:
        process_key - stream id, it is used to define which sending process we are running.

    Returns:
    Raises:
    Todo:
    """

    global _config_from_manager
    global _config

    try:
        _logger.debug("function _retrieve_configuration")

        config_category_name = _CONFIG_CATEGORY_NAME + "_" + str(process_key)

        _event_loop.run_until_complete(configuration_manager.create_category(config_category_name, _DEFAULT_OMF_CONFIG,
                                                                             _CONFIG_CATEGORY_DESCRIPTION))
        _config_from_manager = _event_loop.run_until_complete(configuration_manager.get_category_all_items
                                                              (config_category_name))

        # Retrieves the configurations doing the related conversions
        _config['enable'] = True if _config_from_manager['enable']['value'].upper() == 'TRUE' else False
        _config['duration'] = int(_config_from_manager['duration']['value'])
        _config['source'] = _config_from_manager['source']['value']

        _config['blockSize'] = int(_config_from_manager['blockSize']['value'])
        _config['sleepInterval'] = int(_config_from_manager['sleepInterval']['value'])
        _config['translator'] = _config_from_manager['translator']['value']

    except Exception:
        message = _message_list["e000003"]

        _logger.exception(message)
        raise


def _send_information():
    """

    Args:
    Returns:
    Raises:
    Todo:
    """

    try:
        _logger.debug("function _send_information")

    except Exception:
        message = _message_list["e000006"]

        _logger.exception(message)
        raise


def _plugin_load():
    """ Loads the plugin

    Args:
    Returns:
    Raises:
    Todo:
    """

    global _plugin

    module_to_import = _TRANSLATOR_PATH + _config['translator']

    try:
        _plugin = importlib.import_module(module_to_import)

    except ImportError:
        message = _message_list["e000005"].format(module_to_import)

        _logger.exception(message)
        raise


def _sending_process_init():
    """ Setup the correct state

    Args:
    Returns:
    Raises:
    Todo:
    """

    global _stream_id
    global _plugin
    global _plugin_info

    try:
        prg_text = ", for Linux (x86_64)"

        start_message = "" + _module_name + "" + prg_text + " " + __copyright__ + " "
        _logger.info("{0}".format(start_message))
        _logger.info(_message_list["i000001"])

        _retrieve_configuration(_stream_id)

        _plugin_load()

        _plugin_info = _plugin.retrieve_plugin_info()

        _logger.debug("function _retrieve_configuration " + _plugin_info['name'] + _plugin_info['version'])

    except Exception:
        message = _message_list["e000004"]

        _logger.exception(message)
        raise


if __name__ == "__main__":

    try:
        _logger = logger.setup(__name__)

    except Exception as ex:
        # FIXME:
        tmp_message = _message_list["e000001"].format(str(ex))

        print ("ERROR - ", tmp_message)

    else:
        # Handling input parameters, only one - the stream id is required
        try:
            _stream_id = int(sys.argv[1])
        except:
            tmp_message = _message_list["e000006"].format(str(sys.argv))

            _logger.exception(tmp_message)
            raise InvalidCommandLineParameters(tmp_message)

        try:
            _event_loop = asyncio.get_event_loop()

            _sending_process_init()

            # FIXME:
            _send_information()
            # _event_loop.run_until_complete()

            _logger.info(_message_list["i000002"])

        except Exception as ex:
            tmp_message = _message_list["e000002"].format(str(ex))

            _logger.exception(tmp_message)
