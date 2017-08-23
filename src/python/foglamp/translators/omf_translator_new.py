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


import asyncio
import time

import json
import requests

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

# FIXME:
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

# FIXME:
# statistics
_num_sent = 0
# internal statistic
_num_unsent = 0
"""rows that generate errors in the preparation process, before sending them to OMF"""


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
    "e000020": _module_name + " - cannot complete the retrieval of the plugin information.",
    "e000021": _module_name + " - cannot complete the termination of the OMF translator.",

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
        # FIXME:
        "default": "2"

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

        # FIXME:
        # "type": "JSON",
        # "default": json.dumps({"Location": "S.F."})
    }

}
_CONFIG_CATEGORY_NAME = 'OMF_TRANS'
_CONFIG_CATEGORY_DESCRIPTION = 'Configuration of OMF Translator plugin'

# Configurations retrieved from the Configuration Manager
_config_from_manager = ""
_config = {
    'URL': _DEFAULT_OMF_CONFIG['URL']['default'],
    'producerToken': _DEFAULT_OMF_CONFIG['producerToken']['default'],
    'OMFMaxRetry': int(_DEFAULT_OMF_CONFIG['OMFMaxRetry']['default']),
    'OMFRetrySleepTime': _DEFAULT_OMF_CONFIG['OMFRetrySleepTime']['default'],
    'StaticData': _DEFAULT_OMF_CONFIG['StaticData']['default']
}


class PluginInitializeFailed(RuntimeError):
    """ PluginInitializeFailed """
    pass


def _configuration_retrieve():
    """ Retrieves the configuration from the Configuration Manager

    Returns:
    Raises:
    Todo:
    """

    global _config_from_manager
    global _config

    try:
        _logger.debug("{0} - _configuration_retrieve".format(_module_name))

        _event_loop.run_until_complete(configuration_manager.create_category(_CONFIG_CATEGORY_NAME, _DEFAULT_OMF_CONFIG,
                                                                             _CONFIG_CATEGORY_DESCRIPTION))
        _config_from_manager = _event_loop.run_until_complete(configuration_manager.get_category_all_items
                                                              (_CONFIG_CATEGORY_NAME))

        # Retrieves the configurations and apply the related conversions
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
        plugin_info
    Raises:
    Todo:
    """

    global _logger
    global _event_loop

    try:
        # note : _module_name is used as __name__ refers to the Sending Process
        _logger = logger.setup(_module_name)

    except Exception as e:
        message = _message_list["e000005"].format(str(e))
        current_time = time.strftime("%Y-%m-%d %H:%M:%S:")

        print ("{0} - ERROR - {1}".format(current_time, message))

    try:
        _event_loop = asyncio.get_event_loop()

        _configuration_retrieve()

        plugin_info = {
            'name': "OMF Translator",
            'version': "1.0.0",
            'type': "translator",
            'interface': "1.0",
            'config': _config
        }

    except Exception:
        message = _message_list["e000020"]

        _logger.exception(message)
        raise

    return plugin_info


def plugin_init():
    """ Initializes the OMF plugin for the sending of blocks of readings to the PI Connector.

    Returns:
    Raises:
    Todo:
    """

    try:
        _logger.debug("{0} - plugin_init - URL {1}".format(_module_name, _config['URL']))

        # FIXME:
        # raise

    except Exception:
        message = _message_list["e000006"]

        _logger.exception(message)
        raise PluginInitializeFailed


def plugin_shutdown():
    """ Terminates the OMF plugin

    Returns:
    Raises:
    Todo:
    """

    try:
        _logger.debug("{0} - plugin_shutdown".format(_module_name))

    except Exception:
        message = _message_list["e000021"]

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
        _logger.debug("{0} - transform_in_memory_data START".format(_module_name))

        data_available, new_position, num_sent = transform_in_memory_data(data_to_send, raw_data)

        _logger.debug("{0} - transform_in_memory_data END".format(_module_name))

        if data_available:
            _logger.debug("{0} - send_in_memory_data_to_picromf START".format(_module_name))

            send_in_memory_data_to_picromf("Data", data_to_send)
            data_sent = True

            _logger.debug("{0} - send_in_memory_data_to_picromf END".format(_module_name))

    except Exception:
        message = _message_list["e000004"]

        _logger.exception(message)
        raise

    return data_sent, new_position, num_sent


def send_in_memory_data_to_picromf(message_type, omf_data):
    """Sends data to PICROMF - it retries the operation using a sleep time increased *2 for every retry

    it logs a WARNING only at the end of the retry mechanism

    Args:
        message_type: possible values - Type | Container | Data
        omf_data:     message to send

    Raises:
        Exception: an error occurred during the OMF request

    """

    sleep_time = _config['OMFRetrySleepTime']

    message = ""
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

            response = requests.post(_config['URL'], headers=msg_header, data=omf_data_json, verify=False,
                                     timeout=30)

            # FIXME: temporary/testing code
            if response.status_code == 400:
                raise RuntimeError(response.text)

            _logger.debug("Response |{0}| message: |{1}| |{2}| ".format(message_type,
                                                                        response.status_code,
                                                                        response.text))

            break

        except Exception as e:
            message = _message_list["e000007"].format(e)
            status = 1

            time.sleep(sleep_time)
            num_retry += 1
            sleep_time *= 2

    if status != 0:
        _logger.warning(message)
        raise Exception(message)


def transform_in_memory_data(data_to_send, raw_data):
    """Transforms in memory data into a new structure that could be converted into JSON for PICROMF

    Raises:
        Exception: cannot complete the preparation of the in memory structure.

    """

    global _num_sent
    global _num_unsent

    new_position = 0
    data_available = False

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
                message = _message_list["e000012"].format(tmp_type, e)

                _logger.warning(message)
            else:
                try:
                    transform_in_memory_row(data_to_send, row, measurement_id)

                    # Used for the statistics update
                    _num_sent += 1

                    # Latest position reached
                    new_position = row_id

                    data_available = True

                except Exception as e:
                    _num_unsent += 1

                    message = _message_list["e000013"].format(e)
                    _logger.warning(message)

    except Exception:
        message = _message_list["e000019"]

        _logger.exception(message)
        raise

    return data_available,  new_position, _num_sent


def transform_in_memory_row(data_to_send, row, target_stream_id):
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

            _logger.debug("in memory info |{0}| ".format(new_data))
        else:
            message = _message_list["e000009"]
            _logger.warning(message)

    except Exception:
        message = _message_list["e000010"]

        _logger.exception(message)
        raise


if __name__ == "__main__":
    try:
        # note : _module_name is used as __name__ refers to the Sending Process
        _logger = logger.setup(_module_name)

    except Exception as ex:
        tmp_message = _message_list["e000005"].format(str(ex))
        tmp_current_time = time.strftime("%Y-%m-%d %H:%M:%S:")

        print ("{0} - ERROR - {1}".format(tmp_current_time, tmp_message))

    try:
        _event_loop = asyncio.get_event_loop()

    except Exception as ex:
        tmp_message = _message_list["e000006"].format(str(ex))

        _logger.exception(tmp_message)
        raise
