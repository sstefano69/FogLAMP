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

"""

import resource

import asyncio
import sys
import time
import psycopg2
import importlib
import logging
import datetime

from foglamp import logger, statistics, configuration_manager

# Module information
__author__ = "${FULL_NAME}"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_PERFORMANCE_LOG = True

_MODULE_NAME = "Sending Process"

_MESSAGES_LIST = {

    # Information messages
    "i000001": "Started.",
    "i000002": "Execution completed.",
    "i000003": _MODULE_NAME + " disabled.",

    # Warning / Error messages
    "e000000": "general error",
    "e000001": "cannot setup the logger - error details |{0}|",
    "e000002": "cannot complete the operation - error details |{0}|",
    "e000003": "cannot complete the retrieval of the configuration.",
    "e000004": "cannot complete the initialization.",
    "e000005": "cannot load the plugin |{0}|",
    "e000006": "invalid input parameters, the stream id is required - parameters |{0}|",
    "e000007": "cannot complete the termination of the sending process.",
    "e000008": "unknown data source, it could be only: readings, statistics or audit.",
    "e000009": "cannot complete loading data into the memory.",
    "e000010": "cannot update statistics.",
    "e000011": "invalid input parameters, the stream id is required and it should be a number - parameters |{0}|",
    "e000012": "cannot connect to the DB Layer - error details |{0}|",
    "e000013": "cannot validate the stream id - error details |{0}|",
    "e000014": "multiple streams having same id are defined - stream id |{0}|",
    "e000015": "the selected plugin is not a valid translator - plug in |{0} / {1}|",
    "e000016": "invalid stream id, it is not defined - stream id |{0}|",
}
"""Messages used for Information, Warning and Error notice"""


_TRANSLATOR_PATH = "foglamp.translators."
_TRANSLATOR_TYPE = "translator"

_DATA_SOURCE_READINGS = "readings"
_DATA_SOURCE_STATISTICS = "statistics"
_DATA_SOURCE_AUDIT = "audit"

# FIXME: set proper values
_DEFAULT_OMF_CONFIG = {
    "enable": {
        "description": "A switch that can be used to enable or disable execution of the sending process.",
        "type": "boolean",
        "default": "True"
    },
    "duration": {
        "description": "How long the sending process should run before stopping.",
        "type": "integer",
        "default": "1"
    },
    "source": {
        "description": "Defines the source of the data to be sent on the stream, "
                       "this may be one of either readings, statistics or audit.",
        "type": "string",
        "default": _DATA_SOURCE_READINGS
    },
    "blockSize": {
        "description": "The size of a block of readings to send in each transmission.",
        "type": "integer",
        "default": "4000"
        # "default": "10"
    },
    "sleepInterval": {
        "description": "A period of time, expressed in seconds, "
                       "to wait between attempts to send readings when there are no readings to be sent.",
        "type": "integer",
        "default": "0"
    },
    "translator": {
        "description": "The name of the translator to use to translate the readings "
                       "into the output format and send them",
        "type": "string",
        "default": "omf_translator_new"
        # FIXME:
        # "default": "omf_translator_new2"
        # "default": "empty_translator"
    },

}
_CONFIG_CATEGORY_NAME = 'SEND_PR'
_CONFIG_CATEGORY_DESCRIPTION = 'Configuration of the Sending Process'

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
_DB_CONNECTION_STRING = 'postgresql:///foglamp'

# FIXME:
_pg_conn = ()
_pg_cur = ()

_logger = ""

_event_loop = ""

_stream_id = 0

# Configurations retrieved from the Configuration Manager
_config_from_manager = ""
_config = {
    'enable': _DEFAULT_OMF_CONFIG['enable']['default'],
    'duration': int(_DEFAULT_OMF_CONFIG['duration']['default']),
    'source': _DEFAULT_OMF_CONFIG['source']['default'],
    'blockSize': int(_DEFAULT_OMF_CONFIG['blockSize']['default']),
    'sleepInterval': int(_DEFAULT_OMF_CONFIG['sleepInterval']['default']),
    'translator': _DEFAULT_OMF_CONFIG['translator']['default'],
}


class PluginInitialiseFailed(RuntimeError):
    """ PluginInitializeFailed """
    pass


class UnknownDataSource(RuntimeError):
    """ the data source could be only one among: readings, statistics or audit """
    pass


class InvalidCommandLineParameters(RuntimeError):
    """ Invalid command line parameters, the stream id is the only required """
    pass


def performance_log(func):
    """ Logs information for performance measurement """

    # noinspection PyProtectedMember
    def wrapper(*arg):
        """ wrapper """

        start = datetime.datetime.now()

        if _PERFORMANCE_LOG:
            _logger.info("{0} - PERFORMANCE - {1} START".format(_MODULE_NAME, sys._getframe().f_locals['func']))

        res = func(*arg)

        if _PERFORMANCE_LOG:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            memory_process = (usage[2])/1000

            end = datetime.datetime.now()
            delta = end - start
            delta_milliseconds = int(delta.total_seconds() * 1000)

            _logger.info("{0} - PERFORMANCE - {1} - milliseconds |{2:>6,}| - END"
                         .format(_MODULE_NAME,
                                 sys._getframe().f_locals['func'],
                                 delta_milliseconds))
            _logger.info("{0} - PERFORMANCE - {1} - memory MB |{2:>6,}| - END"
                         .format(_MODULE_NAME,
                                 sys._getframe().f_locals['func'],
                                 memory_process))

        return res

    return wrapper


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
        _message = _MESSAGES_LIST["e000003"]

        _logger.exception(_message)
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
        _message = _MESSAGES_LIST["e000005"].format(module_to_import)

        _logger.exception(_message)
        raise


def _sending_process_init():
    """ Setup the correct state for the Sending Process

    Args:
    Returns:
        False = the sending process is disabled
    Raises:
        PluginInitialiseFailed
    Todo:
    """

    global _plugin
    global _plugin_info
    global _event_loop

    global _pg_conn
    global _pg_cur

    try:
        prg_text = ", for Linux (x86_64)"

        start_message = "" + _MODULE_NAME + "" + prg_text + " " + __copyright__ + " "
        _logger.info("{0}".format(start_message))
        _logger.info(_MESSAGES_LIST["i000001"])

        try:
            _pg_conn = psycopg2.connect(_DB_CONNECTION_STRING)
            _pg_cur = _pg_conn.cursor()

        except Exception as e:
            _message = _MESSAGES_LIST["e000012"].format(str(e))

            _logger.exception(_message)
            raise
        else:
            if is_stream_id_valid(_stream_id):

                _retrieve_configuration(_stream_id)

                if _config['enable']:

                    _plugin_load()

                    _plugin_info = _plugin.retrieve_plugin_info(_stream_id)

                    _logger.debug("{0} - _sending_process_init - {1} - {2} ".format(_MODULE_NAME,
                                                                                    _plugin_info['name'],
                                                                                    _plugin_info['version']))

                    if _is_translator_valid():
                        try:
                            _plugin.plugin_init()

                        except Exception:
                            # FIXME: improve / test
                            raise PluginInitialiseFailed

                else:
                    _message = _MESSAGES_LIST["i000003"]

                    _logger.info(_message)

    except Exception:
        _message = _MESSAGES_LIST["e000004"]

        _logger.exception(_message)
        raise

    return _config['enable']


def _sending_process_shutdown():
    """ Terminates the sending process

    Args:
    Returns:
    Raises:
    Todo:
    """

    global _plugin
    global _pg_conn

    try:
        _plugin.plugin_shutdown()

        _pg_conn.close()

    except Exception:
        _message = _MESSAGES_LIST["e000007"]

        _logger.exception(_message)
        raise


@performance_log
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
        _logger.debug("{0} - _load_data_into_memory_readings".format(_MODULE_NAME))

        sql_cmd = "SELECT id, asset_code, user_ts, reading " \
                  "FROM foglamp.readings " \
                  "WHERE id> {0} " \
                  "ORDER BY id LIMIT {1}" \
            .format(last_object_id, _config['blockSize'])

        _pg_cur.execute(sql_cmd)
        raw_data = _pg_cur.fetchall()

    except Exception:
        _message = _MESSAGES_LIST["e000009"]

        _logger.exception(_message)
        raise

    return raw_data


def _load_data_into_memory_statistics(last_object_id):
    """
    #

    Args:
    Returns:
    Raises:
    Todo: TO BE IMPLEMENTED
    """

    try:
        _logger.debug("{0} - _load_data_into_memory_statistics {1}".format(_MODULE_NAME, last_object_id))

        data_to_send = ""

    except Exception:
        _message = _MESSAGES_LIST["e000006"]

        _logger.exception(_message)
        raise

    return data_to_send


def _load_data_into_memory_audit(last_object_id):
    """
    #

    Args:
    Returns:
    Raises:
    Todo: TO BE IMPLEMENTED
    """

    try:
        _logger.debug("{0} - _load_data_into_memory_audit {1} ".format(_MODULE_NAME, last_object_id))

        data_to_send = ""

    except Exception:
        _message = _MESSAGES_LIST["e000006"]

        _logger.exception(_message)
        raise

    return data_to_send


def _load_data_into_memory(last_object_id):
    """

    Args:
    Returns:
    Raises:
        UnknownDataSource
    Todo:
    """

    try:
        _logger.debug("{0} - _load_data_into_memory".format(_MODULE_NAME))

        if _config['source'] == _DATA_SOURCE_READINGS:
            data_to_send = _load_data_into_memory_readings(last_object_id)

        elif _config['source'] == _DATA_SOURCE_STATISTICS:
            data_to_send = _load_data_into_memory_statistics(last_object_id)

        elif _config['source'] == _DATA_SOURCE_AUDIT:
            data_to_send = _load_data_into_memory_audit(last_object_id)

        else:
            _message = _MESSAGES_LIST["e000008"]
            raise UnknownDataSource(_message)

    except Exception:
        _message = _MESSAGES_LIST["e000006"]

        _logger.exception(_message)
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

    try:
        sql_cmd = "SELECT last_object FROM foglamp.streams WHERE id={0}".format(_stream_id)

        _pg_cur.execute(sql_cmd)
        rows = _pg_cur.fetchall()

        if len(rows) == 0:
            _message = _MESSAGES_LIST["e000016"].format(str(_stream_id))
            raise ValueError(_message)

        elif len(rows) > 1:

            _message = _MESSAGES_LIST["e000014"].format(str(_stream_id))
            raise ValueError(_message)

        else:
            last_object_id = rows[0][0]
            _logger.debug("DB row last_object_id |{0}| : ".format(last_object_id))

    except Exception:
        _message = _MESSAGES_LIST["e000002"]

        _logger.exception(_message)
        raise

    return last_object_id


def is_stream_id_valid(stream_id):
    """ Checks if the stream id provided is valid

    Returns:
        True/False
    Raises:
    Todo:
        it should evolve using the DB layer
    """

    global _pg_cur

    try:
        sql_cmd = "SELECT id FROM foglamp.streams WHERE id={0}".format(stream_id)

        _pg_cur.execute(sql_cmd)
        rows = _pg_cur.fetchall()

        if len(rows) == 0:
            _message = _MESSAGES_LIST["e000016"].format(str(stream_id))
            raise ValueError(_message)

        elif len(rows) > 1:

            _message = _MESSAGES_LIST["e000014"].format(str(stream_id))
            raise ValueError(_message)
        else:
            stream_id_valid = True

    except Exception as e:
        _message = _MESSAGES_LIST["e000013"].format(str(e))

        _logger.exception(_message)
        raise

    return stream_id_valid


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

        sql_cmd = "UPDATE foglamp.streams SET last_object={0}, ts=now()  WHERE id={1}" \
            .format(new_last_object_id, _stream_id)

        _pg_cur.execute(sql_cmd)

        _pg_conn.commit()

    except Exception:
        _message = _MESSAGES_LIST["e000003"]

        _logger.exception(_message)
        raise


def _send_data_block():
    """ sends a block of data to the destination using the configured plugin

    Args:
    Returns:
    Raises:
    Todo:
    """

    data_sent = False
    try:
        _logger.debug("{0} - _send_data_block".format(_MODULE_NAME))

        last_object_id = last_object_id_read()

        data_to_send = _load_data_into_memory(last_object_id)

        if data_to_send:
            try:
                data_sent, new_last_object_id, num_sent = _plugin.plugin_send(data_to_send)

            except Exception:
                # FIXME:
                _message = _MESSAGES_LIST["e000000"]

                _logger.exception(_message)
                raise
            else:
                if data_sent:
                    last_object_id_update(new_last_object_id)

                    update_statistics(num_sent)

    except Exception:
        _message = _MESSAGES_LIST["e000006"]

        _logger.exception(_message)
        raise

    return data_sent


def _send_data():
    """ Handles the sending of the data to the destination using the configured plugin for a defined amount of time

    Args:
    Returns:
    Raises:
    Todo:
    """

    try:
        _logger.debug("{0} - _send_data".format(_MODULE_NAME))

        start_time = time.time()
        elapsed_seconds = 0

        while elapsed_seconds < _config['duration']:

            data_sent = _send_data_block()

            if not data_sent:
                _logger.debug("{0} - _send_data - SLEEPING ".format(_MODULE_NAME))
                time.sleep(_config['sleepInterval'])

            elapsed_seconds = time.time() - start_time
            _logger.debug("{0} - _send_data - elapsed_seconds {1} ".format(_MODULE_NAME, elapsed_seconds))

    except Exception:
        _message = _MESSAGES_LIST["e000006"]

        _logger.exception(_message)
        raise


def _is_translator_valid():
    """ Checks if the translator has adequate characteristics to be used for sending the data

    Args:
    Returns:
        translator_ok: True if the translator is a proper one
    Raises:
    Todo:
    """

    translator_ok = False

    try:
        if _plugin_info['type'] == _TRANSLATOR_TYPE and \
           _plugin_info['name'] != "Empty translator":
            translator_ok = True

    except Exception:
        _message = _MESSAGES_LIST["e000000"]

        _logger.exception(_message)
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
        _message = _MESSAGES_LIST["e000010"]

        _logger.exception(_message)
        raise


if __name__ == "__main__":

    try:
        # FIXME: Development only
        _logger = logger.setup(__name__, level=logging.DEBUG, destination=logger.CONSOLE)
        # _logger = logger.setup(__name__)

    except Exception as ex:
        message = _MESSAGES_LIST["e000001"].format(str(ex))
        current_time = time.strftime("%Y-%m-%d %H:%M:%S:")

        print("{0} - ERROR - {1}".format(current_time, message))
        sys.exit(1)

    # Handling input parameters, only one - the stream id is required, as a number
    try:
        tmp_stream_id = sys.argv[1]
        _stream_id = int(tmp_stream_id)

    except IndexError:
        message = _MESSAGES_LIST["e000006"].format(str(sys.argv))
        _logger.exception(message)
        sys.exit(1)

    except ValueError:
        message = _MESSAGES_LIST["e000011"].format(str(sys.argv))
        _logger.exception(message)
        sys.exit(1)

    else:
        try:
            _event_loop = asyncio.get_event_loop()

            if _sending_process_init():

                if _is_translator_valid():
                    _send_data()
                else:
                    message = _MESSAGES_LIST["e000015"].format(_plugin_info['name'], _plugin_info['type'])
                    _logger.warning(message)

                _sending_process_shutdown()

                _logger.info(_MESSAGES_LIST["i000002"])

            sys.exit(0)

        except Exception as ex:
            message = _MESSAGES_LIST["e000002"].format(str(ex))

            _logger.exception(message)
            sys.exit(1)
