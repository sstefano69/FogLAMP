#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# INTERNAL VERSION : v 1.0.0
# IMPORTANT NOTE   : this version reads rows from the sensor_values_t_new table
#

#
# Import packages
#
import json
import time
import requests
import datetime
import string
import random

from subprocess import call

#
# Import packages - DB operations related
#
import asyncio
import aiopg
import aiopg.sa
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import and_


#FIXME: change the values
server_name    = "WIN-4M7ODKB0RH2"
producer_token = "TI-test3"


object_id       = "_n_15"
sensor          = "TI SensorTag"   + object_id
sensor_location = "S.F."
measurement     = "measurement"    + object_id

type_id             = "15"
type_measurement_id = "type_measurement_" + type_id
type_object_id      = "type_object_id_"   + type_id


#FIXME: tmp
print (call(["uname", "-a"]))
print ("DBG BRK 1")

relay_url = "http://" + server_name  + ":8118/ingress/messages"

# ************************************************************************
def create_data_values_stream_message(target_stream_id, row):
    """
    Creates the data for OMF

    :param target_stream_id: containers
    :param row:              row retrieved from the DB
    """
    data_values_JSON = ''
    #timestamp = datetime.datetime.utcnow().isoformat() + 'Z'
    timestamp = row.created.utcnow().isoformat() + 'Z'

    print("###  ######################################################################################################")
    print ("OMF    : ", end="")
    print ("ID {0} - {1}".format(target_stream_id, str(row.id) ) )

    #sensor_data = json.loads (row.data)
    sensor_data = row.data

    try:
        print("Data full  |{0}| ".format(sensor_data["asset"]))

        value_x = 0
        value_y = 0
        value_z = 0

        value_pressure = 0
        value_lux = 0

        value_humidity = 0
        value_temperature = 0
        value_object = 0
        value_ambient = 0


        ###  ######################################################################################################:
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

        ###  ######################################################################################################:


        data_values_JSON = [
            {
                "containerid": target_stream_id,
                "values": [
                    {
                        "Time": timestamp,
                        "key": row.key,
                        "x": value_x,
                        "y": value_y,
                        "z": value_z,
                        "pressure": value_pressure,
                        "lux": value_lux,

                        "humidity": value_humidity,
                        "temperature": value_temperature,
                        "object": value_object,
                        "ambient": value_ambient,

                    }

                ]
            }
        ]



        print("Data   |{0}| ".format(data_values_JSON ) )
    except:
        print("WARNING : not asset data")

    return data_values_JSON


# ************************************************************************
def sendOMFMessageToEndPoint(message_type, OMF_data):
    try:
        msg_header = {'producertoken': producer_token, 'messagetype': message_type, 'action': 'create',
                      'messageformat': 'JSON', 'omfversion': '1.0'}
        response = requests.post(relay_url, headers=msg_header, data=json.dumps(OMF_data), verify=False, timeout=30)
        print('Response "{0}" message: {1} {2}'.format(message_type, response.status_code,
                                                                                   response.text))
    except Exception as e:
        print(str(datetime.datetime.now()) + " An error ocurred during web request: " + str(e))

def omf_message_update(message_type, OMF_data):
    try:
        msg_header = {'producertoken': producer_token, 'messagetype': message_type, 'action': 'update',
                      'messageformat': 'JSON', 'omfversion': '1.0'}
        response = requests.post(relay_url, headers=msg_header, data=json.dumps(OMF_data), verify=False, timeout=30)
        print('Response "{0}" message: {1} {2}'.format(message_type, response.status_code,
                                                       response.text))
    except Exception as e:
        print(str(datetime.datetime.now()) + " An error ocurred during web request: " + str(e))


# ************************************************************************

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
    "id": measurement,
    "typeid": type_measurement_id
    }
]


staticData = [{
    "typeid": type_object_id,
    "values": [{
        "Name": sensor,
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
            "index": sensor
        }
    }, {
        "source": {
            "typeid": type_object_id,
            "index": sensor
        },
        "target": {
            "containerid": measurement
        }

    }]
}]

# dataValue = [{
#        "containerid": measurement,
#        "values": [{
#                "Time": "2017-01-11T22:23:23.430Z",
#                "keyA": "99.0",
#                "keyB": "98.0"
#        }]
# }]


requests.packages.urllib3.disable_warnings()

### OMF Operations ######################################################################################################:

sendOMFMessageToEndPoint("Type", types)

sendOMFMessageToEndPoint("Container", containers)

sendOMFMessageToEndPoint("Type", types)

sendOMFMessageToEndPoint("Container", containers)

sendOMFMessageToEndPoint("Data", staticData)

sendOMFMessageToEndPoint("Data", linkData)


### DB Operations ######################################################################################################:


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def position_read():
    #return '3947'
    return '4012'


def position_update():
    pass

async def db_read ():
    dsn = 'dbname=foglamp user=foglamp password=foglamp host=127.0.0.1'

    metadata = sa.MetaData()

    _sensor_values_tbl = sa.Table(
        'sensor_values_t_new',
        sa.MetaData(),
        sa.Column('id', sa.BigInteger, primary_key=True),
        sa.Column('created', sa.types.DATETIME),
        sa.Column('key', sa.types.VARCHAR(50)),
        sa.Column('data', JSONB))
    """Defines the table that data will be inserted into"""

    async with aiopg.sa.create_engine (dsn) as engine:
        async with engine.acquire() as conn:
            await conn.execute(_sensor_values_tbl.insert().values(key=id_generator(10)))

            print ("DBG 1")
            position = position_read()

            async for row in conn.execute(_sensor_values_tbl.select().where(_sensor_values_tbl.c.id >= position)):
                print("DB ROW : " ,end="")
                print(row.id, row.created, row.key, row.data,  )

                # Load data into OMS
                values = create_data_values_stream_message(measurement, row )
                sendOMFMessageToEndPoint("Data", values)
                time.sleep(1)

            print("LAST POSITION " + str(row.id) )
            #position = position_read(row.id)



time.sleep(1)
asyncio.get_event_loop().run_until_complete(db_read())