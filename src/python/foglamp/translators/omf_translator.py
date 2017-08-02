#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" Pushes information stored in FogLAMP into OSI/OMF
The information are sent in chunks,
the table foglamp.streams and block_size are used for this handling

.. todo::
   - # TODO: FOGL-203 - the current log mechanism should be substituted.
   - # TODO: FOGL-251 - it should evolve using the DB layer
   - only part of the code is using async

"""

import json
import time
import requests

from foglamp import logger


# Import packages - DB operations
import psycopg2
import asyncio
import aiopg
import aiopg.sa
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from foglamp import statistics, configuration_manager

# Module information
__author__ = "${FULL_NAME}"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

# FIXME: it will be removed using the DB layer
_DB_URL = 'postgresql:///foglamp'
"""DB references"""

_TRACE_EXECUTION = False
"""Enable/Disable info/debug messages"""

_module_name = "OMF Translator"

_message_list = {

    # Information messages
    "i000001": "operation successfully completed",
    "i000002": _module_name + " - Started",
    "i000003": _module_name + " - Execution completed.",

    # Warning / Error messages
    "e000001": _module_name + " - generic error.",
    "e000002": _module_name + " - cannot retrieve the starting point for sending operation - error details |{0}|.",
    "e000003": _module_name + " - cannot update the reached position - error details |{0}|.",
    "e000004": _module_name + " - cannot complete the sending operation - error details |{0}|.",
    "e000005": _module_name + " - cannot configure the logging mechanism. - error details |{0}|.",
    "e000006": _module_name + " - cannot initialize the plugin. - error details |{0}|.",
    "e000007": _module_name + " - an error occurred during the OMF request. - error details |{0}|.",
    "e000008": _module_name + " - an error occurred during the OMF's objects creation. - error details |{0}|.",
    "e000009": _module_name + " - cannot retrieve information about the sensor.",
    "e000010": _module_name + " - unable ro create the JSON message.",
    "e000011": _module_name + " - cannot create the OMF types - error details |{0}|.",
    "e000012": _module_name + " - unknown asset_code - asset |{0}| - error details |{1}|.",
    "e000013": _module_name + " - cannot send sensor information to OMF - error details |{0}|."

}
"""Messages used for Information, Warning and Error notice"""

_logger = ""

_readings_tbl = sa.Table(
    'readings',
    sa.MetaData(),
    sa.Column('id', sa.BigInteger, primary_key=True),
    sa.Column('asset_code', sa.types.VARCHAR(50)),
    sa.Column('read_key', sa.types.VARCHAR(50)),
    sa.Column('user_ts', sa.types.TIMESTAMP),
    sa.Column('reading', JSONB))


# PI Server OMF reference - for detailed information
# http://omf-docs.readthedocs.io/en/v1.0/Data_Msg_Sample.html#data-example

_event_loop = ""

_relay_url = ""
_producer_token = ""

# The size of a block of readings to send in each transmission.
_block_size = 10

# OMF objects creation
_types = ""
_sensor_id = ""
_measurement_id = ""


# OMF object's attributes
_sensor_location = "S.F."

# OMF types definitions - default vales
_type_id = "0"
_type_sensor_id = "type_sensor_id_" + _type_id
_type_measurement_id = "type_measurement_" + _type_id

_OMF_types_definition = []
_sensor_data_keys = []
_sensor_types = []
_sensor_name_type = {}
"""Associates the asset code to the corresponding type"""


_DEFAULT_OMF_CONFIG = {
    "URL": {
        "description": "The URL of the PI Connector to send data to",
        "type": "string",
        "default": "http://WIN-4M7ODKB0RH2:8118/ingress/messages"
    },
    "producerToken": {
        "description": "The producer token that represents this FogLAMP stream",
        "type": "string",
        "default": "omf_translator_b93"

    }
}
_CONFIG_CATEGORY_NAME = 'OMF_TRANS'
_CONFIG_CATEGORY_DESCRIPTION = 'Configuration of OMF Translator plugin'

_config = ""
"""Configurations retrieved from the Configuration Manager"""


# DB operations
_pg_conn = ""
_pg_cur = ""

# statistics
_num_sent = 0
_num_unsent = 0

_num_readings = 0
_num_discarded_readings = 0


def initialize_plugin():
    """Initializes the OMF plugin for the sending of blocks of readings to the PI Connector.

    Retrieves the configuration for :
        relay_url      - URL           - The URL of the PI Connector to send data to.
        producer_token - producerToken - The producer token that represents this FogLAMP stream
        types          - OMFTypes      - A JSON object that contains the OMF type definitions for this stream

    Raises:
        Exception: Fails to initialize the plugin
    """

    global _event_loop
    global _config

    global _relay_url
    global _producer_token
    global _types

    global _type_id
    global _type_sensor_id
    global _type_measurement_id

    global _sensor_data_keys
    global _sensor_types
    global _sensor_name_type

    global _OMF_types_definition

    try:
        _event_loop.run_until_complete(configuration_manager.create_category(_CONFIG_CATEGORY_NAME, _DEFAULT_OMF_CONFIG,
                                                                             _CONFIG_CATEGORY_DESCRIPTION))
        _config = _event_loop.run_until_complete(configuration_manager.get_category_all_items(_CONFIG_CATEGORY_NAME))

        # URL
        _relay_url = _config['URL']['value']

        # OMF types definition - xxx
        _type_id = "150"
        _type_sensor_id = "type_sensor_id_" + _type_id
        _type_measurement_id = "type_measurement_" + _type_id

        # producerToken
        _producer_token = _config['producerToken']['value']

        # OMFTypes
        _sensor_data_keys = ["x", "y", "z", "pressure", "lux", "humidity", "temperature",
                             "object", "ambient", "left", "right", "magnet", "button"]
        """Available proprieties in the reading field"""

        _sensor_types = ["TI_sensorTag_accelerometer",
                         "TI_sensorTag_gyroscope",
                         "TI_sensorTag_magnetometer",
                         "TI_sensorTag_humidity",
                         "TI_sensorTag_luxometer",
                         "TI_sensorTag_pressure",
                         "TI_sensorTag_temperature",

                         "TI_sensorTag_keys",
                         "mouse"
                         ]

        _sensor_name_type = {
            # asset_code                  OMF type
            "TI sensorTag/accelerometer": "TI_sensorTag_accelerometer",
            "TI sensorTag/gyroscope":     "TI_sensorTag_gyroscope",
            "TI sensorTag/magnetometer":  "TI_sensorTag_magnetometer",
            "TI sensorTag/humidity":      "TI_sensorTag_humidity",
            "TI sensorTag/luxometer":     "TI_sensorTag_luxometer",
            "TI sensorTag/pressure":      "TI_sensorTag_pressure",
            "TI sensorTag/temperature":   "TI_sensorTag_temperature",
            "TI sensorTag/keys":          "TI_sensorTag_keys",
            "mouse":                      "mouse"
        }

        _OMF_types_definition = {
            "TI_sensorTag_accelerometer": [
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "static",
                    "properties": {
                        "Name": {
                            "type": "string",
                            "isindex": True
                        },
                        "Location": {
                            "type": "string"
                        }
                    }
                },
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "dynamic",
                    "properties": {
                        "Time": {
                            "format": "date-time",
                            "type": "string",
                            "isindex": True
                        },
                        "x": {
                            "type": "number"
                        },
                        "y": {
                            "type": "number"
                        },
                        "z": {
                            "type": "number"
                        }
                    }
                }
            ],
            "TI_sensorTag_gyroscope": [
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "static",
                    "properties": {
                        "Name": {
                            "type": "string",
                            "isindex": True
                        },
                        "Location": {
                            "type": "string"
                        }
                    }
                },
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "dynamic",
                    "properties": {
                        "Time": {
                            "format": "date-time",
                            "type": "string",
                            "isindex": True
                        },
                        "x": {
                            "type": "number"
                        },
                        "y": {
                            "type": "number"
                        },
                        "z": {
                            "type": "number"
                        }
                    }
                }
            ],
            "TI_sensorTag_magnetometer": [
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "static",
                    "properties": {
                        "Name": {
                            "type": "string",
                            "isindex": True
                        },
                        "Location": {
                            "type": "string"
                        }
                    }
                },
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "dynamic",
                    "properties": {
                        "Time": {
                            "format": "date-time",
                            "type": "string",
                            "isindex": True
                        },
                        "x": {
                            "type": "number"
                        },
                        "y": {
                            "type": "number"
                        },
                        "z": {
                            "type": "number"
                        }
                    }
                }
            ],
            "TI_sensorTag_humidity": [
                {
                    "id":  "xxx",
                    "type": "object",
                    "classification": "static",
                    "properties": {
                        "Name": {
                            "type": "string",
                            "isindex": True
                        },
                        "Location": {
                            "type": "string"
                        }
                    }
                },
                {
                    "id":  "xxx",
                    "type": "object",
                    "classification": "dynamic",
                    "properties": {
                        "Time": {
                            "format": "date-time",
                            "type": "string",
                            "isindex": True
                        },
                        "humidity": {
                            "type": "number"
                        },
                        "temperature": {
                            "type": "number"
                        }
                    }
                }
            ],
            "TI_sensorTag_luxometer": [
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "static",
                    "properties": {
                        "Name": {
                            "type": "string",
                            "isindex": True
                        },
                        "Location": {
                            "type": "string"
                        }
                    }
                },
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "dynamic",
                    "properties": {
                        "Time": {
                            "format": "date-time",
                            "type": "string",
                            "isindex": True
                        },
                        "lux": {
                            "type": "integer"
                        }
                    }
                }
            ],
            "TI_sensorTag_pressure": [
                {
                    "id":  "xxx",
                    "type": "object",
                    "classification": "static",
                    "properties": {
                        "Name": {
                            "type": "string",
                            "isindex": True
                        },
                        "Location": {
                            "type": "string"
                        }
                    }
                },
                {
                    "id":  "xxx",
                    "type": "object",
                    "classification": "dynamic",
                    "properties": {
                        "Time": {
                            "format": "date-time",
                            "type": "string",
                            "isindex": True
                        },
                        "pressure": {
                            "type": "integer"
                        }
                    }
                }
            ],
            "TI_sensorTag_temperature": [
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "static",
                    "properties": {
                        "Name": {
                            "type": "string",
                            "isindex": True
                        },
                        "Location": {
                            "type": "string"
                        }
                    }
                },
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "dynamic",
                    "properties": {
                        "Time": {
                            "format": "date-time",
                            "type": "string",
                            "isindex": True
                        },
                        "object": {
                            "type": "number"
                        },
                        "ambient": {
                            "type": "number"
                        }
                    }
                }
            ],
            "TI_sensorTag_keys": [
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "static",
                    "properties": {
                        "Name": {
                            "type": "string",
                            "isindex": True
                        },
                        "Location": {
                            "type": "string"
                        }
                    }
                },
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "dynamic",
                    "properties": {
                        "Time": {
                            "format": "date-time",
                            "type": "string",
                            "isindex": True
                        },
                        "left": {
                            "type": "string"
                        },
                        "right": {
                            "type": "string"
                        },
                        "magnet": {
                            "type": "string"
                        }
                    }
                }
            ],
            "mouse": [
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "static",
                    "properties": {
                        "Name": {
                            "type": "string",
                            "isindex": True
                        },
                        "Location": {
                            "type": "string"
                        }
                    }
                },
                {
                    "id": "xxx",
                    "type": "object",
                    "classification": "dynamic",
                    "properties": {
                        "Time": {
                            "format": "date-time",
                            "type": "string",
                            "isindex": True
                        },
                        "button": {
                            "type": "string"
                        }
                    }
                }
            ]
        }

    except Exception as e:
        message = _message_list["e000006"].format(e)

        _logger.exception(message)
        raise Exception(message)


def debug_msg_write(severity_message, message):
    """Writes a debug message

    Args:
        severity_message: string related to the severity - "INFO" | "WARNING" | ERROR
        message: message to handle

    Note:
        # TODO FOGL-203 - temporary function that will be removed by FOGL-203
    """

    if _TRACE_EXECUTION:
        if severity_message == "":
            print("{0:}".format(message))
        else:
            print("{0:} - {1:<7} - {2} ".format(time.strftime("%Y-%m-%d %H:%M:%S:"), severity_message, message))
            _logger.exception(message)


def create_data_values_stream_message(target_stream_id, information_to_send):
    """Creates the JSON data for OMF

    Args:
        target_stream_id:     OMF container ID
        information_to_send:  information retrieved that should be prepared for OMF

    Returns:
        data_values_json: information converted in JSON format

    Raises:
        Exception: unable ro create the JSON message.

    """

    data_available = False

    row_id = information_to_send.id
    asset_code = information_to_send.asset_code
    timestamp = information_to_send.user_ts.isoformat()
    sensor_data = information_to_send.reading

    debug_msg_write("INFO", "Stream ID : |{0}| Sensor ID : |{1}| Row ID : |{2}|  "
                    .format(target_stream_id, asset_code, str(row_id)))

    try:
        # Prepares the data for OMF
        data_values_json = [
            {
                "containerid": target_stream_id,
                "values": [
                    {
                        "Time": timestamp
                    }
                ]
            }
        ]

        #
        # Evaluates which data is available
        #
        for data_key in _sensor_data_keys:
            try:
                data_values_json[0]["values"][0][data_key] = sensor_data[data_key]

                data_available = True
            except KeyError:
                pass

        if data_available:
            debug_msg_write("INFO", "OMF Message |{0}| ".format(data_values_json))
        else:
            message = _message_list["e000009"]
            _logger.warning(message)

    except Exception as e:
        message = _message_list["e000010"].format(e)

        _logger.exception(message)
        raise Exception(message)

    return data_values_json


def send_omf_message_to_end_point(message_type, omf_data):
    """Sends data for OMF

    Args:
        message_type: possible values - Type | Container | Data
        omf_data:     message to send

    Raises:
        Exception: an error occurred during the OMF request

    """

    try:
        msg_header = {'producertoken': _producer_token,
                      'messagetype':   message_type,
                      'action':        'create',
                      'messageformat': 'JSON',
                      'omfversion':    '1.0'}

        response = requests.post(_relay_url, headers=msg_header, data=json.dumps(omf_data), verify=False, timeout=30)

        debug_msg_write("INFO", "Response |{0}| message: |{1}| |{2}| ".format(message_type,
                                                                              response.status_code,
                                                                              response.text))

    except Exception as e:
        message = _message_list["e000007"].format(e)

        _logger.exception(message)
        raise Exception(message)


def position_read():
    """Retrieves the starting point for the send operation

    Returns:
        position: starting point for the send operation

    Raises:
        Exception: operations at db level failed

    Todo:
        it should evolve using the DB layer
    """

    global _pg_conn
    global _pg_cur

    position = 0

    try:
        sql_cmd = "SELECT last_object FROM foglamp.streams WHERE id=1"

        _pg_cur.execute(sql_cmd)
        rows = _pg_cur.fetchall()
        for row in rows:
            position = row[0]
            debug_msg_write("INFO", "DB row position |{0}| : ". format(row[0]))

    except Exception as e:
        message = _message_list["e000002"].format(e)

        _logger.exception(message)
        raise Exception(message)

    return position


def position_update(new_position):
    """Updates the handled position

    Args:
        new_position:  Last row already sent to OMF

    Todo:
        it should evolve using the DB layer

    """

    global _pg_conn
    global _pg_cur

    try:
        sql_cmd = "UPDATE foglamp.streams SET last_object={0}, ts=now()  WHERE id=1".format(new_position)
        _pg_cur.execute(sql_cmd)

        _pg_conn.commit()

    except Exception as e:
        message = _message_list["e000003"].format(e)

        _logger.exception(message)
        raise Exception(message)


def omf_types_creation():
    """Creates the types into OMF

    """

    global _sensor_types

    try:
        for sensor_type in _sensor_types:

            type_sensor_id = "type_sensor_id_" + _type_id + "_" + sensor_type
            type_measurement_id = "type_measurement_" + _type_id + "_" + sensor_type

            omf_type = _OMF_types_definition[sensor_type]
            omf_type[0]["id"] = type_sensor_id
            omf_type[1]["id"] = type_measurement_id

            send_omf_message_to_end_point("Type", omf_type)

    except Exception as e:
        message = _message_list["e000011"].format(e)

        _logger.exception(message)
        raise Exception(message)


def omf_objects_creation():
    """Creates all the OMF objects

    Raises:
        Exception: an error occurred during the OMF's objects creation.

    """

    global _sensor_location
    global _sensor_id
    global _measurement_id

    global _type_measurement_id
    global _type_sensor_id

    try:
        for sensor_info in _sensor_name_type:

            _sensor_id = sensor_info
            _measurement_id = "measurement_" + _sensor_id

            tmp_type = _sensor_name_type[_sensor_id]

            _type_sensor_id = "type_sensor_id_" + _type_id + "_" + tmp_type
            _type_measurement_id = "type_measurement_" + _type_id + "_" + tmp_type

            debug_msg_write("INFO", "OMF_object_creation ")
            omf_object_creation()

    except Exception as e:
        message = _message_list["e000008"].format(e)

        _logger.exception(message)
        raise Exception(message)


def omf_object_creation():
    """Creates an object into OMF

    Raises:
        Exception: an error occurred during the OMF's objects creation.

    """

    global _sensor_location
    global _sensor_id
    global _measurement_id

    global _type_measurement_id
    global _type_sensor_id

    try:
        # OSI/OMF objects definition
        containers = [
            {
                "id": _measurement_id,
                "typeid": _type_measurement_id
            }
        ]

        static_data = [{
            "typeid": _type_sensor_id,
            "values": [{
                "Name": _sensor_id,
                "Location": _sensor_location
            }]
        }]

        link_data = [{
            "typeid": "__Link",
            "values": [{
                "source": {
                    "typeid": _type_sensor_id,
                    "index": "_ROOT"
                },
                "target": {
                    "typeid": _type_sensor_id,
                    "index": _sensor_id
                }
            }, {
                "source": {
                    "typeid": _type_sensor_id,
                    "index": _sensor_id
                },
                "target": {
                    "containerid": _measurement_id
                }

            }]
        }]

        send_omf_message_to_end_point("Container", containers)
        send_omf_message_to_end_point("Data", static_data)
        send_omf_message_to_end_point("Data", link_data)

    except Exception as e:
        message = _message_list["e000008"].format(e)

        _logger.exception(message)
        raise Exception(message)


async def send_info_to_omf():
    """Reads the information from the DB and it sends to OMF

    Raises:
        Exception: cannot complete the sending operation

    Todo:
        it should evolve using the DB layer

    """

    global _pg_conn
    global _pg_cur

    global _sensor_id
    global _measurement_id

    global _type_sensor_id
    global _type_measurement_id

    global _num_sent
    global _num_unsent

    info_handled = False

    db_row = ""

    try:
        _pg_conn = psycopg2.connect(_DB_URL)
        _pg_cur = _pg_conn.cursor()

        async with aiopg.sa.create_engine(_DB_URL) as engine:
            async with engine.acquire() as conn:

                    position = position_read()
                    debug_msg_write("INFO", "Last position, already sent |{0}| ".format(str(position)))

                    # Reads the rows from the DB and sends to OMF
                    async for db_row in conn.execute(_readings_tbl.select()
                                                     .where(_readings_tbl.c.id > position)
                                                     .order_by(_readings_tbl.c.id).limit(_block_size)):

                        message = "### sensor information ##################################################"
                        debug_msg_write("INFO", "{0}".format(message))

                        # Identification of the object/sensor
                        _sensor_id = db_row.asset_code
                        _measurement_id = "measurement_" + _sensor_id

                        tmp_type = ""
                        try:
                            # Evaluates if it is a known types
                            tmp_type = _sensor_name_type[_sensor_id]

                        except Exception as e:
                            message = _message_list["e000012"].format(tmp_type, e)

                            _logger.exception(message)
                            debug_msg_write("WARNING", "{0}".format(message))
                        else:
                            debug_msg_write("INFO", "db row |{0}| |{1}| |{2}| ".format(db_row.id,
                                                                                       db_row.user_ts,
                                                                                       db_row.reading))

                            try:
                                # Loads data into the PI Server using OMF
                                values = create_data_values_stream_message(_measurement_id, db_row)
                                send_omf_message_to_end_point("Data", values)

                                # Updates statistics
                                _num_sent += 1

                            except Exception as e:
                                # Updates statistics
                                _num_unsent += 1

                                message = _message_list["e000013"].format(e)
                                _logger.exception(message)

                        info_handled = True

                    message = "### completed ##################################################"
                    debug_msg_write("INFO", "{0}".format(message))

                    if info_handled:
                        new_position = db_row.id
                        debug_msg_write("INFO", "Last position, sent |{0}| ".format(str(new_position)))

                        position_update(new_position)

                        await  _update_statistics()

    except Exception as e:
        message = _message_list["e000004"].format(e)

        _logger.exception(message)
        raise Exception(message)

async def _update_statistics():
    global _num_readings
    global _num_discarded_readings

    await statistics.update_statistics_value('SENT', _num_sent)
    _num_readings = 0
    await statistics.update_statistics_value('UNSENT', _num_unsent)
    _num_discarded_readings = 0

if __name__ == "__main__":

    _logger = logger.setup(__name__)

    prg_text = ", for Linux (x86_64)"

    start_message = "\n" + _module_name +  "" + prg_text + "\n" + __copyright__ + "\n"
    debug_msg_write("", "{0}".format(start_message))
    debug_msg_write("INFO", _message_list["i000002"])

    _event_loop = asyncio.get_event_loop()

    initialize_plugin()

    omf_types_creation()
    omf_objects_creation()

    _event_loop.run_until_complete(send_info_to_omf())

    debug_msg_write("INFO", _message_list["i000003"])
