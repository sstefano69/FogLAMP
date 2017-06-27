# -*- coding: utf-8 -*-
"""Pushes information stored in FogLAMP to OSI/OMF

INTERNAL VERSION : v 1.0.1
IMPORTANT NOTE   : this version reads rows from the sensor_values_t_new table

"""


#
# Import packages
#
import json
import time
import requests
import datetime

#
# Import packages - DB operations
#
import asyncio
import aiopg
import aiopg.sa
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


#
# Constants
#
server_name    = "WIN-4M7ODKB0RH2"
producer_token = "TI-test3"

object_id       = "_n_19"
sensor_location = "S.F."
sensor_id       = "TI SensorTag" + object_id
measurement_id  = "measurement" + object_id

type_id             = "19"
type_measurement_id = "type_measurement_" + type_id
type_object_id      = "type_object_id_"   + type_id

relay_url = "http://" + server_name  + ":8118/ingress/messages"

db_dsn = 'dbname=foglamp user=foglamp password=foglamp host=127.0.0.1'

#
# OSI/OMF objects definition
#

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


def create_data_values_stream_message(target_stream_id, information_to_send):
    """
    Creates the data for OMF

    :param target_stream_id:     container ID
    :param information_to_send:  information retrieved from the DB that should be prepared
    """

    result = True

    data_values_JSON = ''

    sensor_data = information_to_send.data
    row_key     = information_to_send.key
    timestamp   = information_to_send.created.utcnow().isoformat() + 'Z'

    #FIX ME:
    print ("OMF    : ", end="")
    print ("|{0}| - |{1}|".format(target_stream_id, str(information_to_send.id)))
    print ("Sensor ID : |{0}| ".format(sensor_data["asset"]))


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
            value_x = sensor_data["sensor_values"]["x"]
        except:
            pass

        try:
            value_y = sensor_data["sensor_values"]["y"]
        except:
            pass

        try:
            value_z = sensor_data["sensor_values"]["z"]
        except:
            pass

        try:
            value_pressure = sensor_data["sensor_values"]["pressure"]
        except:
            pass

        try:
            value_lux = sensor_data["sensor_values"]["lux"]
        except:
            pass

        try:
            value_lux = sensor_data["sensor_values"]["humidity"]
        except:
            pass

        try:
            value_lux = sensor_data["sensor_values"]["temperature"]
        except:
            pass

        try:
            value_lux = sensor_data["sensor_values"]["object"]
        except:
            pass

        try:
            value_lux = sensor_data["sensor_values"]["ambient"]
        except:
            pass


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


        print("Full data   |{0}| ".format(data_values_JSON ) )
    except:
        result = False
        print("WARNING : not asset data")

    return result, data_values_JSON


def send_OMF_message_to_end_point(message_type, OMF_data):
    """
    Sends data for OMF

    :param message_type: possible values - Type | Container | Data
    :param OMF_data:     message to send
    """

    result = True
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
        result = False
        print(str(datetime.datetime.now()) + " An error occurred during web request: " + str(e))


    return result

def position_read():
    """
    #FIX ME: TB implemented
    Retrieves the starting point for the operation, DB column id.
    """
    #return '3947'
    return '4012'


def position_update():
    """
    #FIX ME: TB implemented
    Updates the handled position, DB column id.
    """
    pass



#
# MAIN
#

#FIX ME:
#requests.packages.urllib3.disable_warnings()

#
# OMF Operations
#

send_OMF_message_to_end_point("Type", types)

send_OMF_message_to_end_point("Container", containers)

send_OMF_message_to_end_point("Type", types)

send_OMF_message_to_end_point("Container", containers)

send_OMF_message_to_end_point("Data", staticData)

send_OMF_message_to_end_point("Data", linkData)


#
# DB Operations
#
async def send_info_to_OMF ():

    row = ""

    _sensor_values_tbl = sa.Table(
        'sensor_values_t_new',
        sa.MetaData(),
        sa.Column('id', sa.BigInteger, primary_key=True),
        sa.Column('created', sa.types.DATETIME),
        sa.Column('key', sa.types.VARCHAR(50)),
        sa.Column('data', JSONB))
    """Defines the table that data will be inserted into"""


    async with aiopg.sa.create_engine (db_dsn) as engine:
        async with engine.acquire() as conn:

            position = position_read()

            # Reads the rows from the DB and sends to OMF
            async for row in conn.execute(_sensor_values_tbl.select().where(_sensor_values_tbl.c.id >= position)):

                # FIX ME: to be removed, only for dev
                print("###  ######################################################################################################")
                print("DB ROW : " ,end="")
                print(row.id, row.created, row.key, row.data,  )

                #FIX ME: to be removed, only for dev
                time.sleep(1)

                # Loads data into OMS
                result, values = create_data_values_stream_message(measurement_id, row)
                if result == True:
                    send_OMF_message_to_end_point("Data", values)

            #FIX ME:
            print(
            "###  ######################################################################################################")
            print("LAST POSITION " + str(row.id) )
            #position = position_read(row.id)


#FIX ME: to be removed, only for dev
time.sleep(1)

asyncio.get_event_loop().run_until_complete(send_info_to_OMF())