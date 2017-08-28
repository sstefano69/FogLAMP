#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" The OMF translator is a plugin output formatter for the FogLAMP appliance.
It is loaded by the send process (see The FogLAMP Sending Process) and runs in the context of the send process,
to send the reading data to a PI Server (or Connector) using the OSIsoft OMF format.

IMPORTANT NOTE : This current version is an empty skeleton.

.. _send_data::

"""

import resource
import datetime
import sys
import asyncio
import time
import json
import requests
import logging

from foglamp import logger, configuration_manager

# Module information
__author__ = "${FULL_NAME}"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_MODULE_NAME = "OMF Translator"

# Defines what and the level of details for logging
_log_debug_level = 0
_log_performance = False

_MESSAGES_LIST = {

    # Information messages
    "i000001": " ",
    "i000002": "Started.",
    "i000003": "Execution completed.",

    # Warning / Error messages
    "e000001": "cannot complete the operation.",
    "e000002": "cannot retrieve the starting point for sending operation.",
    "e000003": "cannot update the reached position.",
    "e000004": "cannot complete the sending operation.",
    "e000005": "cannot setup the logger - error details |{0}|",

    "e000006": "cannot initialize the plugin.",
    "e000007": "an error occurred during the OMF request - error details |{0}|",
    "e000008": "an error occurred during the OMF's objects creation.",
    "e000009": "cannot retrieve information about the sensor.",
    "e000010": "unable to extend the in memory structure with new data.",
    "e000011": "cannot create the OMF types.",
    "e000012": "unknown asset_code - asset |{0}| - error details |{1}|",
    "e000013": "cannot prepare sensor information for PICROMF - error details |{0}|",
    "e000014": "",

    "e000015": "cannot update statistics.",
    "e000016": "",
    "e000017": "cannot complete loading data into the memory.",
    "e000018": "cannot complete the initialization.",
    "e000019": "cannot complete the preparation of the in memory structure.",
    "e000020": "cannot complete the retrieval of the plugin information.",
    "e000021": "cannot complete the termination of the OMF translator.",

}
"""Messages used for Information, Warning and Error notice"""

# FIXME: set proper values
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
        "default": "2"

    },
    "OMFRetrySleepTime": {
        "description": "Seconds between each retry for the communication with the OMF PI Connector Relay",
        "type": "integer",
        "default": "1"

    },
    "OMFHttpTimeout": {
        "description": "Timeout in seconds for the HTTP operations with the OMF PI Connector Relay",
        "type": "integer",
        "default": "30"

    },
    "StaticData": {
        "description": "Static data to include in each sensor reading sent to OMF.",
        "type": "string",
        "default": "Location"

        # "type": "JSON",
        # "default": json.dumps({"Location": "S.F."})
    }

}
_CONFIG_CATEGORY_NAME = 'OMF_TR'
_CONFIG_CATEGORY_DESCRIPTION = 'Configuration of OMF Translator plugin'

# FIXME: tmp code
_asset_code_type = {
    # asset_code                  OMF type
    "TI sensorTag/accelerometer": "TI_sensorTag_accelerometer",
    "TI sensorTag/gyroscope": "TI_sensorTag_gyroscope",
    "TI sensorTag/magnetometer": "TI_sensorTag_magnetometer",
    "TI sensorTag/humidity": "TI_sensorTag_humidity",
    "TI sensorTag/luxometer": "TI_sensorTag_luxometer",
    "TI sensorTag/pressure": "TI_sensorTag_pressure",
    "TI sensorTag/temperature": "TI_sensorTag_temperature",
    "TI sensorTag/keys": "TI_sensorTag_keys",
    "mouse": "mouse"
}

_logger = ""

_event_loop = ""

# Configurations retrieved from the Configuration Manager
_config_from_manager = ""
_config = {
    'URL': _DEFAULT_OMF_CONFIG['URL']['default'],
    'producerToken': _DEFAULT_OMF_CONFIG['producerToken']['default'],
    'OMFMaxRetry': int(_DEFAULT_OMF_CONFIG['OMFMaxRetry']['default']),
    'OMFRetrySleepTime': _DEFAULT_OMF_CONFIG['OMFRetrySleepTime']['default'],
    'OMFHttpTimeout': int(_DEFAULT_OMF_CONFIG['OMFHttpTimeout']['default']),
    'StaticData': _DEFAULT_OMF_CONFIG['StaticData']['default']
}


class URLFetchError(RuntimeError):
    """ URLFetchError """
    pass


class PluginInitializeFailed(RuntimeError):
    """ PluginInitializeFailed """
    pass


def performance_log(func):
    """ Logs information for performance measurement """

    # noinspection PyProtectedMember
    def wrapper(*arg):
        """ wrapper """

        start = datetime.datetime.now()

        # Code execution
        res = func(*arg)

        if _log_performance:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            memory_process = (usage[2])/1000

            delta = datetime.datetime.now() - start
            delta_milliseconds = int(delta.total_seconds() * 1000)

            _logger.info("PERFORMANCE - {0} - milliseconds |{1:>6,}| - memory MB |{2:>6,}|"
                         .format(sys._getframe().f_locals['func'],
                                 delta_milliseconds,
                                 memory_process))

        return res

    return wrapper


def _configuration_retrieve(_stream_id):
    """ Retrieves the configuration from the Configuration Manager

    Returns:
    Raises:
    Todo:
    """

    global _config_from_manager
    global _config

    try:
        _logger.debug("{0} - _configuration_retrieve".format(_MODULE_NAME))

        config_category_name = _CONFIG_CATEGORY_NAME + "_" + str(_stream_id)

        _event_loop.run_until_complete(configuration_manager.create_category(config_category_name, _DEFAULT_OMF_CONFIG,
                                                                             _CONFIG_CATEGORY_DESCRIPTION))
        _config_from_manager = _event_loop.run_until_complete(configuration_manager.get_category_all_items
                                                              (config_category_name))

        # Retrieves the configurations and apply the related conversions
        _config['URL'] = _config_from_manager['URL']['value']
        _config['producerToken'] = _config_from_manager['producerToken']['value']
        _config['OMFMaxRetry'] = int(_config_from_manager['OMFMaxRetry']['value'])
        _config['OMFRetrySleepTime'] = int(_config_from_manager['OMFRetrySleepTime']['value'])
        _config['OMFHttpTimeout'] = int(_config_from_manager['OMFHttpTimeout']['value'])
        _config['StaticData'] = _config_from_manager['StaticData']['value']

    except Exception:
        message = _MESSAGES_LIST["e000003"]

        _logger.exception(message)
        raise


def retrieve_plugin_info(_stream_id):
    """ Allows the device service to retrieve information from the plugin

    Returns:
        plugin_info
    Raises:
    Todo:
    """

    global _logger
    global _event_loop

    try:
        # note : _module_name is used as __name__ refers to the Sending Process
        # FIXME: Development only
        if _log_debug_level == 0:
            _logger = logger.setup(_MODULE_NAME, level=logging.INFO, destination=logger.CONSOLE)
            # _logger = logger.setup(_MODULE_NAME)

        elif _log_debug_level >= 1:
            _logger = logger.setup(_MODULE_NAME, level=logging.DEBUG, destination=logger.CONSOLE)

    except Exception as e:
        message = _MESSAGES_LIST["e000005"].format(str(e))
        current_time = time.strftime("%Y-%m-%d %H:%M:%S:")

        print ("{0} - ERROR - {1}".format(current_time, message))

    try:
        _event_loop = asyncio.get_event_loop()

        _configuration_retrieve(_stream_id)

        plugin_info = {
            'name': "OMF Translator",
            'version': "1.0.0",
            'type': "translator",
            'interface': "1.0",
            'config': _config
        }

    except Exception:
        message = _MESSAGES_LIST["e000020"]

        _logger.exception(message)
        raise

    return plugin_info


def plugin_init():
    """ Initializes the OMF plugin for the sending of blocks of readings to the PI Connector.

    Returns:
    Raises:
        PluginInitializeFailed
    Todo:
    """

    try:
        _logger.debug("{0} - plugin_init - URL {1}".format(_MODULE_NAME, _config['URL']))

        # FIXME: tmp code
        # raise

    except Exception:
        message = _MESSAGES_LIST["e000006"]

        _logger.exception(message)
        raise PluginInitializeFailed


def plugin_shutdown():
    """ Terminates the OMF plugin

    Returns:
    Raises:
    Todo:
    """

    try:
        _logger.debug("{0} - plugin_shutdown".format(_MODULE_NAME))

    except Exception:
        message = _MESSAGES_LIST["e000021"]

        _logger.exception(message)
        raise PluginInitializeFailed


def plugin_send(raw_data):
    """ Translates and sends to the destination system the data provided by the Sending Process

    Returns:
        data_to_send

    Raises:
    Todo:
    """

    data_to_send = []
    data_sent = False

    try:
        data_available, new_position, num_sent = _transform_in_memory_data(data_to_send, raw_data)

        if data_available:
            _send_in_memory_data_to_picromf("Data", data_to_send)
            data_sent = True

    except Exception:
        message = _MESSAGES_LIST["e000004"]

        _logger.exception(message)
        raise

    return data_sent, new_position, num_sent


@performance_log
def _send_in_memory_data_to_picromf(message_type, omf_data):
    """Sends data to PICROMF - it retries the operation using a sleep time increased *2 for every retry

    it logs a WARNING only at the end of the retry mechanism

    Args:
        message_type: possible values - Type | Container | Data
        omf_data:     _message to send

    Raises:
        Exception: an error occurred during the OMF request

    """

    sleep_time = _config['OMFRetrySleepTime']

    _message = ""
    status = 0
    num_retry = 1

    while num_retry < _config['OMFMaxRetry']:
        try:
            status = 0
            msg_header = {'producertoken': _config['producerToken'],
                          'messagetype': message_type,
                          'action': 'create',
                          'messageformat': 'JSON',
                          'omfversion': '1.0'}

            omf_data_json = json.dumps(omf_data)

            response = requests.post(_config['URL'],
                                     headers=msg_header,
                                     data=omf_data_json,
                                     verify=False,
                                     timeout=_config['OMFHttpTimeout'])

            # FIXME: temporary/testing code
            if response.status_code == 400:
                tmp_text = str(response.status_code) + " " + response.text
                _message = _MESSAGES_LIST["e000007"].format(tmp_text)
                raise URLFetchError(_message)

            _logger.debug("Response |{0}| _message: |{1}| |{2}| ".format(message_type,
                                                                         response.status_code,
                                                                         response.text))

            break

        except Exception as e:
            _message = _MESSAGES_LIST["e000007"].format(e)
            status = 1

            time.sleep(sleep_time)
            num_retry += 1
            sleep_time *= 2

    if status != 0:
        _logger.warning(_message)
        # FIXME:
        raise TimeoutError(_message)


@performance_log
def _transform_in_memory_data(data_to_send, raw_data):
    """Transforms in memory data into a new structure that could be converted into JSON for PICROMF

    Raises:
        Exception: cannot complete the preparation of the in memory structure.

    """

    new_position = 0
    data_available = False

    # statistics
    num_sent = 0

    # internal statistic - rows that generate errors in the preparation process, before sending them to OMF
    num_unsent = 0

    try:
        for row in raw_data:

            row_id = row[0]
            asset_code = row[1]

            # Identification of the object/sensor
            measurement_id = "measurement_" + asset_code

            tmp_type = ""
            try:
                # Evaluates if it is a known asset code
                tmp_type = _asset_code_type[asset_code]

            except Exception as e:
                message = _MESSAGES_LIST["e000012"].format(tmp_type, e)

                _logger.warning(message)
            else:
                try:
                    _transform_in_memory_row(data_to_send, row, measurement_id)

                    # Used for the statistics update
                    num_sent += 1

                    # Latest position reached
                    new_position = row_id

                    data_available = True

                except Exception as e:
                    num_unsent += 1

                    message = _MESSAGES_LIST["e000013"].format(e)
                    _logger.warning(message)

    except Exception:
        message = _MESSAGES_LIST["e000019"]

        _logger.exception(message)
        raise

    return data_available, new_position, num_sent


def _transform_in_memory_row(data_to_send, row, target_stream_id):
    """Extends the in memory structure using data retrieved from the Storage Layer

    Args:
        data_to_send:      data to send - updated/used by reference
        row:               information retrieved from the Storage Layer that it is used to extend data_to_send
        target_stream_id:  OMF container ID

    Raises:
        Exception: unable to extend the in memory structure with new data.

    """

    data_available = False

    try:
        row_id = row[0]
        asset_code = row[1]
        timestamp = row[2].isoformat()
        sensor_data = row[3]

        if _log_debug_level == 2:
            _logger.debug("Stream ID : |{0}| Sensor ID : |{1}| Row ID : |{2}|  "
                          .format(target_stream_id, asset_code, str(row_id)))

        # Prepares new data for the PICROMF
        new_data = [
            {
                "containerid": target_stream_id,
                "values": [
                    {
                        "Time": timestamp
                    }
                ]
            }
        ]

        # Evaluates which data is available
        for data_key in sensor_data:
            try:
                new_data[0]["values"][0][data_key] = sensor_data[data_key]

                data_available = True
            except KeyError:
                pass

        if data_available:
            # note : append produces an not properly constructed OMF message
            data_to_send.extend(new_data)

            if _log_debug_level == 2:
                _logger.debug("in memory info |{0}| ".format(new_data))

        else:
            message = _MESSAGES_LIST["e000009"]
            _logger.warning(message)

    except Exception:
        message = _MESSAGES_LIST["e000010"]

        _logger.exception(message)
        raise


if __name__ == "__main__":
    try:
        # note : _module_name is used as __name__ refers to the Sending Process
        # FIXME: Development only
        if _log_debug_level:
            _logger = logger.setup(_MODULE_NAME, level=logging.DEBUG, destination=logger.CONSOLE)
        else:
            _logger = logger.setup(_MODULE_NAME, level=logging.INFO, destination=logger.CONSOLE)
            # _logger = logger.setup(_MODULE_NAME)

    except Exception as ex:
        tmp_message = _MESSAGES_LIST["e000005"].format(str(ex))
        tmp_current_time = time.strftime("%Y-%m-%d %H:%M:%S:")

        print ("{0} - ERROR - {1}".format(tmp_current_time, tmp_message))

    try:
        _event_loop = asyncio.get_event_loop()

    except Exception as ex:
        tmp_message = _MESSAGES_LIST["e000006"].format(str(ex))

        _logger.exception(tmp_message)
        raise
