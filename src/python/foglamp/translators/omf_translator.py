#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Pushes information stored in FogLAMP into OSI/OMF
The information are sent in chunks,
the table foglamp.streams and the constant block_size are used for this handling

NOTE   :
    - this version reads rows from the foglamp.readings table - Latest FogLAMP code
    - it uses foglamp.streams to track the information to send
    - block_size identifies the number of rows to send for each execution

#FIXME:
    - only part of the code is using async and SA

    - Temporary SQL code used for dev :

        INSERT INTO foglamp.destinations (id,description, ts ) VALUES (1,'OMF', now() );

        INSERT INTO foglamp.streams (id,destination_id,description, reading_id,ts ) VALUES (1,1,'OMF', 666,now());

        SELECT * FROM foglamp.streams;

        SELECT * FROM foglamp.destinations;

        UPDATE foglamp.streams SET reading_id=4194 WHERE id=1;


"""



#
# Import packages
#
import json
import time
import requests
import sys
import datetime


#
# Import packages - DB operations
#
import psycopg2
import asyncio
import aiopg
import aiopg.sa
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

#
# Global Constants
#
#

# Module information
PRG_NAME          = "OMF Translator"
PRG_VERSION       = "1.0.14"
PRG_STAGE         = "dev"                  # dev/rel
PRG_TEXT          = ", for Linux (x86_64)"
PRG_COMPANY       = "2017 DB SOFTWARE INC."

# DB references
DB_URL     = 'postgresql://foglamp:foglamp@localhost:5432/foglamp'

#
# Global variables
#

# PI Server references
server_name    = ""
relay_url      = ""
producer_token = ""

# Handling the information in chunks
block_size = 5

# OMF Types
types = ""

# Types definitions
type_id             = "3"
type_measurement_id = "omf_trans_type_measurement_" + type_id
type_object_id      = "omf_trans_type_object_id_" + type_id

# Object's attributes
sensor_location = "S.F."

# DB operations
pg_conn = ""
pg_cur  = ""


#
# Functions
#
def plugin_initialize():
    """
    Initialise the OMF plugin for the sending of blocks of readings to the PI Connector.

    Retrieves the configuration for :
        URL           - The URL of the PI Connector to send data to.
        producerToken - The producer token that represents this FogLAMP stream
        OMFTypes      - A JSON object that contains the OMF type definitions for this stream


    """

    global server_name
    global relay_url
    global producer_token
    global types

    status = True

    try:
        # URL
        server_name = "WIN-4M7ODKB0RH2"
        relay_url = "http://" + server_name + ":8118/ingress/messages"

        # producerToken
        producer_token = "omf_translator_55"

        # OMFTypes
        types = [
            {
                "id": type_object_id,
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
    except:
        status = False

    return status

def msg_write( severityMessage,
               message
               ):
    """
    Writes a message on the screen
    """

    print ("{0:} - {1:<7} - {2} ".format(time.strftime("%Y-%m-%d %H:%M:%S:"), severityMessage ,message) )



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
    #FIXME:
    #timestamp   = information_to_send.user_ts.isoformat() + 'Z'
    timestamp   = information_to_send.user_ts.isoformat()
    sensor_data = information_to_send.reading

    msg_write("INFO", "Stream ID : |{0}| ".format(target_stream_id))
    msg_write("INFO", "Sensor ID : |{0}| ".format(asset_code))
    msg_write("INFO", "Row    ID : |{0}| ".format(str(row_id)))

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
            msg_write("INFO", "OMF Message   |{0}| ".format(data_values_JSON))

        else:
            status = False
            msg_write("WARNING ", "not asset data")



    except:
        status = False
        msg_write("WARNING ", "not asset data")

    return status, data_values_JSON


def send_OMF_message_to_end_point(message_type, OMF_data):
    """
    Sends data for OMF

    :param message_type: possible values - Type | Container | Data
    :param OMF_data:     message to send
    """

    status = True
    try:
        msg_header = {'producertoken': producer_token,
                      'messagetype': message_type,
                      'action': 'create',
                      'messageformat': 'JSON',
                      'omfversion': '1.0'}

        response = requests.post(relay_url, headers=msg_header, data=json.dumps(OMF_data), verify=False, timeout=30)

        msg_write("INFO", "Response |{0}| message: |{1}| |{2}| ".format(message_type,
                                                                        response.status_code,
                                                                        response.text))


    except Exception as e:
        status = False
        msg_write("ERROR ", "An error occurred during web request: {0}". format (e) )
        

    return status

def position_read():
    """
    Retrieves the starting point for the operation

    #FIXME: it should evolve using the DB layer
    """

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
            msg_write("INFO", "DB row position |{0}| : ". format (row[0]))

    except:
        status = False

    return status, position




def position_update(new_position):
    """
    Updates the handled position

    :param new_position:  Last row already sent to OMF

    #FIXME: it should evolve using the DB layer
    """

    global pg_conn
    global pg_cur

    status    = True

    try:
        sql_cmd = "UPDATE foglamp.streams SET reading_id={0}  WHERE id=1".format(new_position)
        pg_cur.execute(sql_cmd)

        pg_conn.commit()

    except:
        message = sys.exc_info()[1]
        #msg_write("ERROR", "Position update || ")
        msg_write("ERROR", "Position update |{0}| ".format(message))
        status = False


    return status

def OMF_types_creation ():
    """
    Creates the types into OMF
    """
    global types

    status = True

    status = send_OMF_message_to_end_point("Type", types)

    return status


def OMF_object_creation ():
    """
    Creates an object into OMF
    """

    global sensor_location
    global sensor_id
    global measurement_id

    global type_measurement_id
    global type_object_id


    # OSI/OMF objects definition
    containers = [
        {
            "id": measurement_id,
            "typeid": type_measurement_id
        }
    ]

    staticData = [{
        "typeid": type_object_id,
        "values": [{
            "Name": sensor_id,
            "Location": sensor_location
        }]
    }]

    linkData = [{
        "typeid": "__Link",
        "values": [{
            "source": {
                "typeid": type_object_id,
                "index": "_ROOT"
            },
            "target": {
                "typeid": type_object_id,
                "index": sensor_id
            }
        }, {
            "source": {
                "typeid": type_object_id,
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


async def send_info_to_OMF ():
    """
    Reads the information from the DB and it sends to OMF

    #FIXME: it should evolve using the DB layer
    """

    global pg_conn
    global pg_cur

    global sensor_id
    global measurement_id

    db_row = ""

    pg_conn = psycopg2.connect(DB_URL)
    pg_cur  = pg_conn.cursor()

    _sensor_values_tbl = sa.Table(
        'readings',
        sa.MetaData(),
        sa.Column('id', sa.BigInteger, primary_key=True),
        sa.Column('asset_code', sa.types.VARCHAR(50)),
        sa.Column('read_key', sa.types.VARCHAR(50)),
        sa.Column('user_ts', sa.types.TIMESTAMP),
        sa.Column('reading', JSONB))
    """Defines the table that data will be inserted into"""

    async with aiopg.sa.create_engine (DB_URL) as engine:
        async with engine.acquire() as conn:

            status, position = position_read()

            msg_write("INFO", "Last position, already sent |{0}| ".format (str (position))  )

            # Reads the rows from the DB and sends to OMF
            #FIXME: ordr by user_ts and the limit aren't working properly
            #async for db_row in conn.execute(_sensor_values_tbl.select().where(_sensor_values_tbl.c.id > position).order_by(_sensor_values_tbl.c.user_ts).limit(block_size) ):
            async for db_row in conn.execute(_sensor_values_tbl.select().where(_sensor_values_tbl.c.id > position).order_by(_sensor_values_tbl.c.id).limit(block_size) ):


                message =  "### sensor information ######################################################################################################"
                msg_write("INFO", "{0}".format(message) )

                # Identification of the object/sensor
                object_id      = db_row.asset_code
                sensor_id      = object_id
                measurement_id = "measurement_" + object_id

                OMF_object_creation ()

                msg_write("INFO", "db row |{0}| |{1}| |{2}| |{3}| ".format(db_row.id, db_row.user_ts, db_row.read_key, db_row.reading  ))

                # Loads data into OMS
                status, values = create_data_values_stream_message(measurement_id, db_row)
                if status == True:
                    send_OMF_message_to_end_point("Data", values)

            message = "### completed ######################################################################################################"
            msg_write("INFO", "{0}".format(message))

            try:
                new_position = db_row.id
                msg_write("INFO", "Last position, sent |{0}| ".format(str(new_position)))

                status = position_update (new_position)
            except:
                pass


#
# MAIN
#
start_message    = "\n" + PRG_NAME + " - Ver " + PRG_VERSION + "" + PRG_TEXT + "\n" + PRG_COMPANY + "\n"
print (start_message)

status = plugin_initialize()

if status == True:
    #FIX ME: TBD
    #requests.packages.urllib3.disable_warnings()

    # OMF - creations of the types
    OMF_types_creation ()

    asyncio.get_event_loop().run_until_complete(send_info_to_OMF())