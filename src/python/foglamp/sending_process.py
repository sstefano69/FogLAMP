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

.. _send_data::

"""


import asyncio
import sys
import time
import psycopg2

from foglamp import logger, statistics, configuration_manager

import importlib

# Module information
__author__ = "${FULL_NAME}"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_TRANSLATOR_PATH = "translators."
_TRANSLATOR_TYPE = "translator"

# Plugin handling - loading an empty plugin
_module_template = _TRANSLATOR_PATH + "empty_translator"
_plugin = importlib.import_module(_module_template)
_plugin_info = {
    'name': "",
    'version': "",
    'type': "",
    'interface': "",
    'config': ""
}

# DB references
# FIXME: it will be removed using the DB layer
_DB_CONNECTION_STRING = 'postgresql:///foglamp'
_pg_conn = ""
_pg_cur = ""

_module_name = "Sending Process"

_message_list = {

    # Information messages
    "i000001": _module_name + " - Started.",
    "i000002": _module_name + " - Execution completed.",

    # Warning / Error messages
    "e000000": _module_name + " - general error",
    "e000001": _module_name + " - cannot setup the logger - error details |{0}|",
    "e000002": _module_name + " - cannot complete the operation - error details |{0}|",
    "e000003": _module_name + " - cannot complete the retrieval of the configuration.",
    "e000004": _module_name + " - cannot complete the initialization.",
    "e000005": _module_name + " - cannot load the plugin |{0}|",
    "e000006": _module_name + " - invalid input parameters, the stream id is required - parameters |{0}|",
    "e000007": _module_name + " - cannot complete the termination of the sending process.",
    "e000008": _module_name + " - unknown data source, it could be only: readings, statistics or audit.",
    "e000009": _module_name + " - cannot complete loading data into the memory.",
    "e000010": _module_name + " - cannot update statistics.",
}
"""Messages used for Information, Warning and Error notice"""

_logger = ""

_event_loop = ""

_stream_id = 0

# For the update of the statistics
_num_sent = 0

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


class PluginInitializeFailed(RuntimeError):
    """ PluginInitializeFailed """
    pass


class UnknownDataSource(RuntimeError):
    """ the data source could be only: readings, statistics or audit """
    pass


class InvalidCommandLineParameters(RuntimeError):
    """ Invalid command line parameters, the stream id is required """
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

        # Retrieves the configurations and apply the related conversions
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
        raise ImportError


def _sending_process_init():
    """ Setup the correct state for the Sending Process

    Args:
    Returns:
    Raises:
    Todo:
    """

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

        _logger.debug("{0} - _sending_process_init - {1} - {2} ".format(_module_name,
                                                                        _plugin_info['name'],
                                                                        _plugin_info['version']))

        # FIXME:
        if _is_translator_ok():
            try:
                _plugin.plugin_init()

            except Exception:
                # FIXME:
                raise PluginInitializeFailed

    except Exception:
        message = _message_list["e000004"]

        _logger.exception(message)
        raise


def _sending_process_shutdown():
    """ Terminates the sending process

    Args:
    Returns:
    Raises:
    Todo:
    """

    global _plugin

    try:
        _plugin.plugin_shutdown()

    except Exception:
        message = _message_list["e000007"]

        _logger.exception(message)
        raise


def _load_data_into_memory_readings(last_object_id):
    """ Extracts from the DB Layer data related to the readings loading into the memory

    Args:
    Returns:
        raw_data: data extracted from the DB Layer
    Raises:
    Todo:
    """

    global _pg_cur

    try:
        _logger.debug("{0} - _load_data_into_memory_readings".format(_module_name))

        sql_cmd = "SELECT id, asset_code, user_ts, reading FROM foglamp.readings WHERE id> {0} ORDER BY id LIMIT {1}"\
                  .format(last_object_id, _config['blockSize'])

        _pg_cur.execute(sql_cmd)
        raw_data = _pg_cur.fetchall()

    except Exception:
        message = _message_list["e000009"]

        _logger.exception(message)
        raise

    return raw_data


def _load_data_into_memory_statistics():
    """
    # FIXME:

    Args:
    Returns:
    Raises:
    Todo:
    """

    try:
        _logger.debug("{0} - _load_data_into_memory_readings".format(_module_name))

        data_to_send = ""

    except Exception:
        message = _message_list["e000006"]

        _logger.exception(message)
        raise

    return data_to_send


def _load_data_into_memory_audit():
    """
    # FIXME:

    Args:
    Returns:
    Raises:
    Todo:
    """

    try:
        _logger.debug("{0} - _load_data_into_memory_readings".format(_module_name))

        data_to_send = ""

    except Exception:
        message = _message_list["e000006"]

        _logger.exception(message)
        raise

    return data_to_send


def _load_data_into_memory(last_object_id):
    """

    Args:
    Returns:
    Raises:
    Todo:
    """

    # FIXME:

    try:
        _logger.debug("{0} - _load_data_into_memory".format(_module_name))

        if _config['source'] == "readings":
            data_to_send = _load_data_into_memory_readings(last_object_id)

        elif _config['source'] == "statistics":
            data_to_send = _load_data_into_memory_statistics()

        elif _config['source'] == "audit":
            data_to_send = _load_data_into_memory_audit()

        else:
            message = _message_list["e000008"]
            raise UnknownDataSource(message)

    except Exception:
        message = _message_list["e000006"]

        _logger.exception(message)
        raise

    return data_to_send


def last_object_id_read():
    """Retrieves the starting point for the send operation

    Returns:
        last_object_id: starting point for the send operation

    Raises:
        Exception: operations at db level failed

    Todo:
        it should evolve using the DB layer
    """

    global _pg_cur

    last_object_id = 0

    try:
        sql_cmd = "SELECT last_object FROM foglamp.streams WHERE id={0}".format(_stream_id)

        _pg_cur.execute(sql_cmd)
        rows = _pg_cur.fetchall()
        for row in rows:
            last_object_id = row[0]
            _logger.debug("DB row last_object_id |{0}| : ". format(row[0]))

    except Exception:
        message = _message_list["e000002"]

        _logger.exception(message)
        raise

    return last_object_id


def last_object_id_update(new_last_object_id):
    """Updates reached position in the communication with PICROMF

    Args:
        new_last_object_id:  Last row already sent to the PICROMF

    Todo:
        it should evolve using the DB layer

    """
    global _pg_cur
    global _pg_conn

    try:
        _logger.debug("Last position, sent |{0}| ".format(str(new_last_object_id)))

        sql_cmd = "UPDATE foglamp.streams SET last_object={0}, ts=now()  WHERE id={1}"\
            .format(new_last_object_id, _stream_id)

        _pg_cur.execute(sql_cmd)

        _pg_conn.commit()

    except Exception:
        message = _message_list["e000003"]

        _logger.exception(message)
        raise


def _send_data():
    """ Handles the sending of the data to the destination using the configured plugin

    Args:
    Returns:
    Raises:
    Todo:
    """

    global _pg_conn
    global _pg_cur

    try:
        _logger.debug("{0} - _send_data".format(_module_name))

        _pg_conn = psycopg2.connect(_DB_CONNECTION_STRING)
        _pg_cur = _pg_conn.cursor()

        last_object_id = last_object_id_read()

        data_to_send = _load_data_into_memory(last_object_id)

        try:
            data_sent, new_last_object_id, num_sent = _plugin.plugin_send(data_to_send)

        except Exception:
            # FIXME:
            message = _message_list["e000000"]

            _logger.exception(message)
            raise
        else:
            if data_sent:
                last_object_id_update(new_last_object_id)

                update_statistics(num_sent)

    except Exception:
        message = _message_list["e000006"]

        _logger.exception(message)
        raise


def _is_translator_ok():
    """ Checks if the translator_ok has adequate characteristics to be used for sending the data

    Args:
    Returns:
        translator_ok: True if the translator_ok is a proper one
    Raises:
    Todo:
    """

    translator_ok = False

    try:
        if _plugin_info['type'] == _TRANSLATOR_TYPE:
            translator_ok = True

    except Exception:
        message = _message_list["e000000"]

        _logger.exception(message)
        raise

    return translator_ok


def update_statistics(num_sent):
    """Updates FogLAMP statistics

    Raises :
        Exception - cannot update statistics
    """

    try:
        _event_loop.run_until_complete(statistics.update_statistics_value('SENT', num_sent))

    except Exception:
        message = _message_list["e000010"]

        _logger.exception(message)
        raise


if __name__ == "__main__":

    try:
        _logger = logger.setup(__name__)

    except Exception as ex:
        tmp_message = _message_list["e000001"].format(str(ex))
        current_time = time.strftime("%Y-%m-%d %H:%M:%S:")

        print ("{0} - ERROR - {1}".format(current_time, tmp_message))

    # Handling input parameters, only one - the stream id is required
    try:
        _stream_id = int(sys.argv[1])
    except Exception:
        tmp_message = _message_list["e000006"].format(str(sys.argv))

        _logger.exception(tmp_message)
        raise InvalidCommandLineParameters(tmp_message)

    try:
        _event_loop = asyncio.get_event_loop()

        _sending_process_init()

        if _is_translator_ok():
            _send_data()

        _sending_process_shutdown()

        _logger.info(_message_list["i000002"])

    except Exception as ex:
        tmp_message = _message_list["e000002"].format(str(ex))

        _logger.exception(tmp_message)
