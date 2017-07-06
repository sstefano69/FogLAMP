#!/usr/bin/env python3

# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" Pushes information stored in FogLAMP into OSI/OMF
The information are sent in chunks,
the table foglamp.streams and the constant block_size are used for this handling

Note   :
    - this version reads rows from the foglamp.readings table - Latest FogLAMP code
    - it uses foglamp.streams to track the information to send
    - block_size identifies the number of rows to send for each execution

Todo:
    - it should evolve using the DB layer
    - only part of the code is using async and SA

"""

# Import packages
import json
import time
import requests
import sys
import datetime

import logging
import logging.handlers

# Import packages - DB operations
import psycopg2
import asyncio
import aiopg
import aiopg.sa
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# Module information
__module__    = "OMF Translator"
__prg_text__  = ", for Linux (x86_64)"
__company__   = "2017 DB SOFTWARE INC."

__author__    = "2017 DB SOFTWARE INC."
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__   = "Apache 2.0"
__version__   = "1.0.15"

DB_URL     = 'postgresql://foglamp:foglamp@localhost:5432/foglamp'
"""DB references"""

_message_list  = {
    # Info messages
    "i000001": "operation successfully completed",
    "i000002": "Started",
    "i000003": "Execution completed.",

    # Warning / Error messages
    "e000001": __module__ + " - generic error.",
    "e000002": __module__ + " - cannot retrieve the starting point for sending operation - error details |{0}|.",
    "e000003": __module__ + " - cannot update the reached position - error details |{0}|.",
    "e000004": __module__ + " - execution terminated - error details |{0}|.",
    "e000005": __module__ + " - cannot configure the logging mechanism. - error details |{0}|.",
    "e000006": __module__ + " - cannot initialize the plugin. - error details |{0}|.",
    "e000007": __module__ + " - an error occurred during the OMF request. - error details |{0}|.",
    "e000008": __module__ + " - an error occurred during the OMF's objects creation. - error details |{0}|."

}
"""Used messages"""

# Logger
_log       = ""
_handler   = ""
_formatter = ""



_log_screen = True
"""Enable/Disable screen messages"""

# SA related
_readings_values_tbl = sa.Table(
    'readings',
    sa.MetaData(),
    sa.Column('id', sa.BigInteger, primary_key=True),
    sa.Column('asset_code', sa.types.VARCHAR(50)),
    sa.Column('read_key', sa.types.VARCHAR(50)),
    sa.Column('user_ts', sa.types.TIMESTAMP),
    sa.Column('reading', JSONB))
"""Contains the information to send to OMF"""


# PI Server references
_server_name    = ""
_relay_url      = ""
_producer_token = ""

# The size of a block of readings to send in each transmission.
block_size = 20

# OMF's objects creation
_types           = ""
sensor_id       = ""
measurement_id  = ""

_containers     = ""
_static_data    = ""
_link_data      = ""

# OMF object's attributes
sensor_location = "S.F."

# OMF's types definitions
type_id             = "6"
type_measurement_id = "omf_trans_type_measurement_" + type_id
type_sensor_id      = "omf_trans_type_sensor_id_"   + type_id


# DB operations
pg_conn = ""
pg_cur  = ""


#
# Functions
#
def plugin_initialize():
    """Initialise the OMF plugin for the sending of blocks of readings to the PI Connector.

    Retrieves the configuration for :
        relay_url      - URL           - The URL of the PI Connector to send data to.
        producer_token - producerToken - The producer token that represents this FogLAMP stream
        types          - OMFTypes      - A JSON object that contains the OMF type definitions for this stream

    Returns:
        status: True if successful, False otherwise.

    Raises:
        Exception: Fails to initialise the plugin
    """

    global _server_name
    global _relay_url
    global _producer_token
    global _types
    global _containers
    global _static_data
    global _link_data

    global sensor_location
    global sensor_id
    global measurement_id

    global type_measurement_id
    global type_sensor_id

    status = True

    try:
        # URL
        _server_name = "WIN-4M7ODKB0RH2"
        _relay_url   = "http://" + _server_name + ":8118/ingress/messages"

        # producerToken
        _producer_token = "omf_translator_61"

        # OMFTypes
        _types = [
            {
                "id": type_sensor_id,
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
                "id": type_measurement_id,
                "type": "object",
                "classification": "dynamic",
                "properties": {
                    "Time": {
                        "format": "date-time",
                        "type": "string",
                        "isindex": True
                    },
                    "key": {
                        "type": "string"
                    },
                    "x": {
                        "type": "number"
                    },
                    "y": {
                        "type": "number"
                    },
                    "z": {
                        "type": "number"
                    },
                    "pressure": {
                        "type": "integer"
                    },
                    "lux": {
                        "type": "integer"
                    },
                    "humidity": {
                        "type": "number"
                    },
                    "temperature": {
                        "type": "number"
                    },
                    "object": {
                        "type": "number"
                    },
                    "ambient": {
                        "type": "number"
                    }

                }
            }
        ]


        # OSI/OMF object definition
        _containers = [
            {
                "id": measurement_id,
                "typeid": type_measurement_id
            }
        ]

        _static_data = [{
            "typeid": type_sensor_id,
            "values": [{
                "Name": sensor_id,
                "Location": sensor_location
            }]
        }]

        _link_data = [{
            "typeid": "__Link",
            "values": [{
                "source": {
                    "typeid": type_sensor_id,
                    "index": "_ROOT"
                },
                "target": {
                    "typeid": type_sensor_id,
                    "index": sensor_id
                }
            }, {
                "source": {
                    "typeid": type_sensor_id,
                    "index": sensor_id
                },
                "target": {
                    "containerid": measurement_id
                }

            }]
        }]


    except Exception as e:
        status  = False
        message = _message_list["e000006"].format(e)

        _log.error(message)
        raise Exception(message)

    return status


def debug_msg_write(severityMessage, message):
    """Writes a debug message
    """

    global _log_screen

    if _log_screen == True:

        if severityMessage == "":
            print("{0:}".format(message))
        else:
            print ("{0:} - {1:<7} - {2} ".format(time.strftime("%Y-%m-%d %H:%M:%S:"), severityMessage ,message) )

    _log.debug(message)

    return


def create_data_values_stream_message(target_stream_id, information_to_send):
    """
    Creates the data for OMF

    :param target_stream_id:     OMF container ID
    :param information_to_send:  information retrieved from the DB that should be prepared

    :return:  status            = True | False
    :return:  data_values_JSON  = information_to_send converted in JSON format
    """

    status = True
    data_available = False

    data_values_JSON = ''

    row_id      = information_to_send.id
    row_key     = information_to_send.read_key
    asset_code  = information_to_send.asset_code
    timestamp   = information_to_send.user_ts.isoformat()
    sensor_data = information_to_send.reading

    debug_msg_write("INFO", "Stream ID : |{0}| ".format(target_stream_id))
    debug_msg_write("INFO", "Sensor ID : |{0}| ".format(asset_code))
    debug_msg_write("INFO", "Row    ID : |{0}| ".format(str(row_id)))

    try:
        # Prepares the data for OMF
        data_values_JSON = [
            {
                "containerid": target_stream_id,
                "values": [
                    {
                        "Time": timestamp,
                        "key": row_key
                    }
                ]
            }
        ]

        #
        # Evaluates which data is available
        #
        try:
            data_values_JSON[0]["values"][0]["x"] = sensor_data["x"]
            data_available = True
        except:
            pass

        try:
            data_values_JSON[0]["values"][0]["y"] = sensor_data["y"]
            data_available = True
        except:
            pass

        try:
            data_values_JSON[0]["values"][0]["z"] = sensor_data["z"]
            data_available = True
        except:
            pass

        try:
            data_values_JSON[0]["values"][0]["pressure"] = sensor_data["pressure"]
            data_available = True
        except:
            pass

        try:
            data_values_JSON[0]["values"][0]["lux"] = sensor_data["lux"]
            data_available = True
        except:
            pass

        try:
            data_values_JSON[0]["values"][0]["humidity"] = sensor_data["humidity"]
            data_available = True
        except:
            pass

        try:
            data_values_JSON[0]["values"][0]["temperature"] = sensor_data["temperature"]
            data_available = True
        except:
            pass

        try:
            data_values_JSON[0]["values"][0]["object"] = sensor_data["object"]
            data_available = True
        except:
            pass

        try:
            data_values_JSON[0]["values"][0]["ambient"] = sensor_data["ambient"]
            data_available = True
        except:
            pass



        if data_available == True:
            debug_msg_write("INFO", "OMF Message   |{0}| ".format(data_values_JSON))

        else:
            status = False
            debug_msg_write("WARNING ", "not asset data")



    except:
        status = False
        debug_msg_write("WARNING ", "not asset data")

    return status, data_values_JSON


def send_OMF_message_to_end_point(message_type, OMF_data):
    """Sends data for OMF

    Args:
        message_type: possible values - Type | Container | Data
        OMF_data:     message to send

    Returns:
        status:    True if successful, False otherwise.

    Raises:
        Exception: an error occurred during the OMF request

    """

    status = True

    try:
        msg_header = {'producertoken': _producer_token,
                      'messagetype':   message_type,
                      'action':        'create',
                      'messageformat': 'JSON',
                      'omfversion':    '1.0'}

        response = requests.post(_relay_url, headers=msg_header, data=json.dumps(OMF_data), verify=False, timeout=30)

        debug_msg_write("INFO", "Response |{0}| message: |{1}| |{2}| ".format(message_type,
                                                                              response.status_code,
                                                                              response.text))


    except Exception as e:
        status  = False
        message = _message_list["e000007"].format(e)

        _log.error(message)
        raise Exception(message)

    return status



def log_configuration():
    """Configures the log mechanism

    Returns:
        status:    True if successful, False otherwise.

    Raises:
        Exception: Fails to configure the log

    """

    global _log
    global _handler
    global _formatter

    status = True

    try:
        _log = logging.getLogger(__module__)

        _log.setLevel(logging.DEBUG)
        _handler = logging.handlers.SysLogHandler(address='/dev/log')  # /var/run/syslog

        _formatter = logging.Formatter('%(module)s.%(funcName)s: %(message)s')
        _handler.setFormatter(_formatter)

        _log.addHandler(_handler)

    except Exception as e:
        status = False
        raise Exception(_message_list["e000005"].format(e))


    return status


def position_read():
    """Retrieves the starting point for the send operation

    Returns:
        status:    True if successful, False otherwise.
        position:

    Raises:
        Exception: operations at db level failed

    Todo:
        it should evolve using the DB layer
    """

    global log
    global pg_conn
    global pg_cur

    status    = True
    position  = 0

    try:
        sql_cmd = "SELECT reading_id FROM foglamp.streams WHERE id=1"

        pg_cur.execute (sql_cmd)
        rows = pg_cur.fetchall()
        for row in rows:
            position = row[0]
            debug_msg_write("INFO", "DB row position |{0}| : ". format (row[0]))


    except Exception as e:
        status  = False
        message = _message_list["e000002"].format(e)

        _log.error(message)
        raise Exception(message)


    return status, position

def position_update(new_position):
    """Updates the handled position

    Args:
        new_position:  Last row already sent to OMF

    Returns:
        status: True if successful, False otherwise.

    Todo:
        it should evolve using the DB layer

    """

    global pg_conn
    global pg_cur

    status    = True

    try:
        sql_cmd = "UPDATE foglamp.streams SET reading_id={0}, ts=now()  WHERE id=1".format(new_position)
        pg_cur.execute(sql_cmd)

        pg_conn.commit()

    except Exception as e:
        status  = False
        message =_message_list["e000003"].format(e)

        _log.error(message)
        raise Exception(message)

    return status

def OMF_types_creation ():
    """Creates the types into OMF

    Returns:
        status: True if successful, False otherwise.

    """
    global _types

    status = True

    status = send_OMF_message_to_end_point("Type", _types)

    return status


def OMF_object_creation ():
    """Creates an object into OMF

    Returns:
        status: True if successful, False otherwise.

    Raises:
        Exception: an error occurred during the OMF's objects creation.

    """

    status = True


    global sensor_location
    global sensor_id
    global measurement_id

    global type_measurement_id
    global type_sensor_id


    # OSI/OMF objects definition
    containers = [
        {
            "id": measurement_id,
            "typeid": type_measurement_id
        }
    ]

    staticData = [{
        "typeid": type_sensor_id,
        "values": [{
            "Name": sensor_id,
            "Location": sensor_location
        }]
    }]

    linkData = [{
        "typeid": "__Link",
        "values": [{
            "source": {
                "typeid": type_sensor_id,
                "index": "_ROOT"
            },
            "target": {
                "typeid": type_sensor_id,
                "index": sensor_id
            }
        }, {
            "source": {
                "typeid": type_sensor_id,
                "index": sensor_id
            },
            "target": {
                "containerid": measurement_id
            }

        }]
    }]


    status = send_OMF_message_to_end_point("Container", containers)

    if status == True:
        status = send_OMF_message_to_end_point("Data", staticData)

    if status == True:
        status = send_OMF_message_to_end_point("Data", linkData)

    return status



    #FIXME:
    # try:
    #     send_OMF_message_to_end_point("Container", _containers)
    #
    #     send_OMF_message_to_end_point("Data", _static_data)
    #
    #     send_OMF_message_to_end_point("Data", _link_data)
    #
    #
    # except Exception as e:
    #     status  = False
    #     message =_message_list["e000008"].format(e)
    #
    #     _log.error(message)
    #     raise Exception(message)

    return status


async def send_info_to_OMF ():
    """Reads the information from the DB and it sends to OMF

    Returns:
        status: True if successful, False otherwise.

    Raises:
        Exception:

    Todo:
        it should evolve using the DB layer

    """


    global pg_conn
    global pg_cur

    global sensor_id
    global measurement_id

    db_row = ""

    try:
        pg_conn = psycopg2.connect(DB_URL)
        pg_cur  = pg_conn.cursor()

        # FIXME:
        status = position_update(97996)

        async with aiopg.sa.create_engine (DB_URL) as engine:
            async with engine.acquire() as conn:

                    status, position = position_read()
                    debug_msg_write("INFO", "Last position, already sent |{0}| ".format(str(position)))

                    # Reads the rows from the DB and sends to OMF
                    #FIXME: order by user_ts and the limit aren't working properly
                    #async for db_row in conn.execute(_readings_values_tbl.select().where(_readings_values_tbl.c.id > position).order_by(_readings_values_tbl.c.user_ts).limit(block_size) ):
                    async for db_row in conn.execute(_readings_values_tbl.select().where(_readings_values_tbl.c.id > position).order_by(_readings_values_tbl.c.id).limit(block_size) ):

                        message =  "### sensor information ######################################################################################################"
                        debug_msg_write("INFO", "{0}".format(message))

                        # Identification of the object/sensor
                        sensor_id      = db_row.asset_code
                        measurement_id = "measurement_" + sensor_id

                        status = OMF_object_creation ()

                        debug_msg_write("INFO", "db row |{0}| |{1}| |{2}| |{3}| ".format(db_row.id, db_row.user_ts, db_row.read_key, db_row.reading))

                        # Loads data into OMF
                        status, values = create_data_values_stream_message(measurement_id, db_row)
                        if status == True:
                            status = send_OMF_message_to_end_point("Data", values)

                    message = "### completed ######################################################################################################"
                    debug_msg_write("INFO", "{0}".format(message))

                    try:
                        new_position = db_row.id
                        debug_msg_write("INFO", "Last position, sent |{0}| ".format(str(new_position)))

                        status = position_update (new_position)
                    except:
                        pass

    except Exception as e:
        status  = False
        message = _message_list["e000004"].format(e)

        _log.error(message)
        raise Exception(message)

#
# MAIN
#
log_configuration ()

start_message    = "\n" + __module__ + " - Ver " + __version__ + "" + __prg_text__ + "\n" + __company__ + "\n"
debug_msg_write ("", "{0}".format(start_message) )
debug_msg_write ("INFO", _message_list["e000002"])

plugin_initialize()



#FIX ME: TBD
#requests.packages.urllib3.disable_warnings()

# OMF - creations of the types
OMF_types_creation ()

asyncio.get_event_loop().run_until_complete(send_info_to_OMF())

debug_msg_write ("INFO", _message_list["e000003"])
