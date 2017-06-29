#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Pushes information stored in FogLAMP into OSI/OMF
The information are sent in chunks,
the table foglamp.omf_trans_position and the constant block_size are used for this handling

INTERNAL VERSION : v 1.0.4

NOTE   :
    - this version reads rows from the foglamp.readings table - Latest FogLAMP code
    - it uses foglamp.omf_trans_position to track the information to send
    - block_size (currently at 5) identifies the number of rows to send for each execution

#FIXME:
    - only part of the code is using async and SA
    - there are some time.sleep for development purpose

    - Temporary SQL code used for dev :
        create table foglamp.omf_trans_position
        (
            id bigint
        );

        TRUNCATE TABLE foglamp.omf_trans_position;
        INSERT INTO foglamp.omf_trans_position (id) VALUES (666);

        UPDATE foglamp.omf_trans_position SET id=666;
        SELECT * FROM foglamp.omf_trans_position;
"""


#
# Import packages
#
import json
import time
import requests
import datetime
import sys

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
# Constants
#
server_name    = "WIN-4M7ODKB0RH2"
producer_token = "omf_translator_3"

sensor_location = "S.F."

type_id             = "3"
type_measurement_id = "omf_trans_type_measurement_" + type_id
type_object_id      = "omf_trans_type_object_id_"   + type_id

relay_url = "http://" + server_name  + ":8118/ingress/messages"

# DB rekated
#FIXME: port=5432'
db_dsn     = 'dbname=foglamp user=foglamp password=foglamp host=127.0.0.1'
block_size = 5
#FIXME: TBD
db_dsn_sa  = 'postgresql://foglamp:foglamp@localhost:5432/foglamp'


# Global variables
conn = ""
cur  = ""


def create_data_values_stream_message(target_stream_id, information_to_send):
    """
    Creates the data for OMF

    :param target_stream_id:     OMF container ID
    :param information_to_send:  information retrieved from the DB that should be prepared
    """

    status = True
    data_available = False

    data_values_JSON = ''

    row_id      = information_to_send.id
    row_key     = information_to_send.read_key
    asset_code  = information_to_send.asset_code
    timestamp   = information_to_send.user_ts.isoformat() + 'Z'
    sensor_data = information_to_send.reading


    #FIX ME:
    print ("OMF    : ", end="")
    print ("|{0}| - |{1}|".format(target_stream_id, str(row_id)))
    print ("Sensor ID : |{0}| ".format(asset_code))


    try:
        value_x = 0
        value_y = 0
        value_z = 0

        value_pressure = 0
        value_lux = 0

        value_humidity = 0
        value_temperature = 0
        value_object = 0
        value_ambient = 0

        #
        # Evaluates which data is available
        #
        try:
            value_x = sensor_data["x"]
            data_available = True
        except:
            pass

        try:
            value_y = sensor_data["y"]
            data_available = True
        except:
            pass

        try:
            value_z = sensor_data["z"]
            data_available = True
        except:
            pass

        try:
            value_pressure = sensor_data["pressure"]
            data_available = True
        except:
            pass

        try:
            value_lux = sensor_data["lux"]
            data_available = True
        except:
            pass

        try:
            value_lux = sensor_data["humidity"]
            data_available = True
        except:
            pass

        try:
            value_lux = sensor_data["temperature"]
            data_available = True
        except:
            pass

        try:
            value_lux = sensor_data["object"]
            data_available = True
        except:
            pass

        try:
            value_lux = sensor_data["ambient"]
            data_available = True
        except:
            pass

        if data_available == True:
            # Prepares the data for OMF
            data_values_JSON = [
                {
                    "containerid":         target_stream_id,
                    "values": [
                        {
                            "Time":        timestamp,
                            "key":         row_key,

                            "x":           value_x,
                            "y":           value_y,
                            "z":           value_z,
                            "pressure":    value_pressure,
                            "lux":         value_lux,
                            "humidity":    value_humidity,
                            "temperature": value_temperature,
                            "object":      value_object,
                            "ambient":     value_ambient,
                        }
                    ]
                }
            ]

            print("Full data   |{0}| ".format(data_values_JSON))
        else:
            status = False
            print("WARNING : not asset data")


    except:
        status = False
        print("WARNING : not asset data")

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

        print('Response "{0}" message: {1} {2}'.format(message_type,
                                                       response.status_code,
                                                       response.text))

    except Exception as e:
        status = False
        print(str(datetime.datetime.now()) + " An error occurred during web request: " + str(e))


    return status

def position_read():
    """
    Retrieves the starting point for the operation, DB column id.

    #FIXME: it should evolve using SA/ASYNC
    """

    global conn
    global cur

    status    = True
    position  = 0

    try:
        sql_cmd = "SELECT id FROM foglamp.omf_trans_position"


        cur.execute (sql_cmd)
        rows = cur.fetchall()
        for row in rows:
            position = row[0]
            print ("ROW Position {:>10,} : ". format (row[0]))
    except:
        status = False

    return status, position




def position_update(new_position):
    """
    Updates the handled position, DB column id.

    :param new_position:  Last row already sent to OMF

    #FIXME: it should evolve using SA/ASYNC
    """

    status    = True

    try:
        conn = psycopg2.connect(db_dsn)
        cur = conn.cursor()

        sql_cmd  = "UPDATE foglamp.omf_trans_position SET id={0}".format(new_position )
        cur.execute(sql_cmd)

        conn.commit()
    except:
        status = False

    return status

def OMF_types_creation ():
    """
    Creates the type into OMF
    """
    status = True

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


    status = send_OMF_message_to_end_point("Type", types)

    return status


def OMF_object_creation ():
    """
    Creates the object into OMF
    """

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

#
# MAIN
#

#FIX ME:
#requests.packages.urllib3.disable_warnings()

#
# OMF Operations
#
OMF_types_creation ()

#
# DB Operations
#
async def send_info_to_OMF ():

    global conn
    global cur

    global object_id
    global sensor_id
    global measurement_id


    db_row = ""

    conn = psycopg2.connect(db_dsn)
    cur = conn.cursor()

    _sensor_values_tbl = sa.Table(
        'readings',
        sa.MetaData(),
        sa.Column('id', sa.BigInteger, primary_key=True),
        sa.Column('asset_code', sa.types.VARCHAR(50)),
        sa.Column('read_key', sa.types.VARCHAR(50)),
        sa.Column('user_ts', sa.types.TIMESTAMP),
        sa.Column('reading', JSONB))
    """Defines the table that data will be inserted into"""

    async with aiopg.sa.create_engine (db_dsn) as engine:
        async with engine.acquire() as conn:

            status, position = position_read()

            print("LAST POSITION ALREDY SENT" + str (position) )

            # Reads the rows from the DB and sends to OMF
            async for db_row in conn.execute(_sensor_values_tbl.select().where(_sensor_values_tbl.c.id > position).limit(block_size).order_by(_sensor_values_tbl.c.id) ):

                print( "###  ######################################################################################################")

                # Identification of the object/sensor
                object_id      = db_row.asset_code
                sensor_id      = "sensor_"      + object_id
                measurement_id = "measurement_" + object_id

                OMF_object_creation ()

                # FIX ME: to be removed, only for dev

                print("DB ROW : " ,end="")
                print(db_row.id, db_row.user_ts, db_row.read_key, db_row.reading,  )

                #FIX ME: to be removed, only for dev
                time.sleep(1)

                # Loads data into OMS
                status, values = create_data_values_stream_message(measurement_id, db_row)
                if status == True:
                    send_OMF_message_to_end_point("Data", values)

            #FIX ME:
            print("###  ######################################################################################################")
            new_position = db_row.id
            print("LAST POSITION SENT " + str(new_position) )
            status = position_update (new_position)


#FIX ME: to be removed, only for dev
time.sleep(1)

asyncio.get_event_loop().run_until_complete(send_info_to_OMF())