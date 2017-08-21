#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" The OMF translator is a plugin output formatter for the FogLAMP appliance.
It is loaded by the send process (see The FogLAMP Sending Process) and runs in the context of the send process,
to send the reading data to a PI Server (or Connector) using the OSIsoft OMF format.

IMPORTANT NOTE : This current version is an empty skeleton.

.. _send_information::

"""

import asyncio

from foglamp import logger, configuration_manager

# Module information
__author__ = "${FULL_NAME}"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

# FIXME: it will be removed using the DB layer
_DB_URL = 'postgresql:///foglamp'
"""DB references"""

_module_name = "OMF Translator"

_message_list = {

    # Information messages
    "i000001": " ",
    "i000002": _module_name + " - Started.",
    "i000003": _module_name + " - Execution completed.",

    # Warning / Error messages
    "e000001": _module_name + " - cannot complete the operation.",
    "e000002": _module_name + " - cannot retrieve the starting point for sending operation.",
    "e000003": _module_name + " - cannot update the reached position.",
    "e000004": _module_name + " - cannot complete the sending operation.",
    "e000005": _module_name + " - cannot setup the logger - error details |{0}|",

    "e000006": _module_name + " - cannot initialize the plugin.",
    "e000007": _module_name + " - an error occurred during the OMF request - error details |{0}|",
    "e000008": _module_name + " - an error occurred during the OMF's objects creation.",
    "e000009": _module_name + " - cannot retrieve information about the sensor.",
    "e000010": _module_name + " - unable to extend the in memory structure with new data.",
    "e000011": _module_name + " - cannot create the OMF types.",
    "e000012": _module_name + " - unknown asset_code - asset |{0}| - error details |{1}|",
    "e000013": _module_name + " - cannot prepare sensor information for PICROMF - error details |{0}|",
    "e000014": _module_name + " - ",

    "e000015": _module_name + " - cannot update statistics.",
    "e000016": _module_name + " - ",
    "e000017": _module_name + " - cannot complete loading data into the memory.",
    "e000018": _module_name + " - cannot complete the initialization.",
    "e000019": _module_name + " - cannot complete the preparation of the in memory structure.",

}
"""Messages used for Information, Warning and Error notice"""

_logger = ""

_event_loop = ""

_DEFAULT_OMF_CONFIG = {
    "URL": {
        "description": "The URL of the PI Connector to send data to",
        "type": "string",
        "default": "http://WIN-4M7ODKB0RH2:8118/ingress/messages"
    },
    "producerToken": {
        "description": "The producer token that represents this FogLAMP stream",
        "type": "string",
        "default": "omf_translator_502"

    },
    "OMFMaxRetry": {
        "description": "Max number of retries for the communication with the OMF PI Connector Relay",
        "type": "integer",
        "default": "5"

    },
    "OMFRetrySleepTime": {
        "description": "Seconds between each retry for the communication with the OMF PI Connector Relay",
        "type": "integer",
        "default": "1"

    },
    "StaticData": {
        "description": "Static data to include in each sensor reading sent to OMF.",
        "type": "string",
        "default": "Location"
    }

}
_CONFIG_CATEGORY_NAME = 'OMF_TRANS'
_CONFIG_CATEGORY_DESCRIPTION = 'Configuration of OMF Translator plugin'

# Configurations retrieved from the Configuration Manager
_config_from_manager = ""
_config = {
    'URL': _DEFAULT_OMF_CONFIG['URL']['default'],
    'producerToken': _DEFAULT_OMF_CONFIG['producerToken']['default'],
    'OMFMaxRetry': _DEFAULT_OMF_CONFIG['OMFMaxRetry']['default'],
    'OMFRetrySleepTime': _DEFAULT_OMF_CONFIG['OMFRetrySleepTime']['default'],
    'StaticData': _DEFAULT_OMF_CONFIG['StaticData']['default']
}


def _configuration_retrieve():
    """ Retrieves the configuration from the Configuration Manager

    Returns:
    Raises:
    Todo:
    """

    global _config_from_manager
    global _config

    try:
        _logger.debug("PLUG IN - _retrieve_configuration")

        _event_loop.run_until_complete(configuration_manager.create_category(_CONFIG_CATEGORY_NAME, _DEFAULT_OMF_CONFIG,
                                                                             _CONFIG_CATEGORY_DESCRIPTION))
        _config_from_manager = _event_loop.run_until_complete(configuration_manager.get_category_all_items
                                                              (_CONFIG_CATEGORY_NAME))

        # Retrieves the configurations doing the related conversions
        _config['URL'] = _config_from_manager['URL']['value']
        _config['producerToken'] = _config_from_manager['producerToken']['value']
        _config['OMFMaxRetry'] = int(_config_from_manager['OMFMaxRetry']['value'])
        _config['OMFRetrySleepTime'] = int(_config_from_manager['OMFRetrySleepTime']['value'])
        _config['StaticData'] = _config_from_manager['StaticData']['value']

    except Exception:
        message = _message_list["e000003"]

        _logger.exception(message)
        raise


def retrieve_plugin_info():
    """ Allows the device service to retrieve information from the plugin

    Returns:
    Raises:
    Todo:
    """

    global _logger
    global _event_loop

    try:
        _logger = logger.setup(_module_name)

    except Exception as ex:
        tmp_message = _message_list["e000005"].format(str(ex))

        print ("ERROR - ", tmp_message)
        raise
    else:
        try:
            _event_loop = asyncio.get_event_loop()

        except Exception as ex:
            tmp_message = _message_list["e000001"].format(str(ex))

            _logger.exception(tmp_message)
            raise

        _configuration_retrieve()

        plugin_info = {
            'name': "OMF Translator",
            'version': "1.0.0",
            'type': "translator",
            'interface': "1.0",
            'config': _config
        }

    return plugin_info


def plugin_init():
    """ Initializes the OMF plugin for the sending of blocks of readings to the PI Connector.

    Returns:
    Raises:
    Todo:
    """

    try:
        print("OMF translator init -  OK ")

    except Exception:
        message = _message_list["e000006"]

        _logger.exception(message)
        raise


if __name__ == "__main__":

    _logger = logger.setup(__name__)

    _event_loop = asyncio.get_event_loop()
