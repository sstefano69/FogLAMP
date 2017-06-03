# Import packages
import sys, json, random, time, platform, socket, datetime
import threading

# This name will be automatically populated (or you can hard-code it) this is the name
#  of the PI AF Element that will be created, and it'll be included in the names
#  of PI Points that get created as well


device_name = (socket.gethostname()) + ""


stream_id_for_sending_values = ""

producer_token = "OMF";
def sendInitialOMFMessages(deviceId):
    # Update the global variable that contains the unique device ID
    device_name = deviceId

    # Create variables to store the Ids of the streams that will be used
    global stream_id_for_sending_values
    stream_id_for_sending_values = (device_name + "_data_values_stream")

# ************************************************************************
#  Define a helper function to allow easily sending web request messages
#  this function can later be customized to allow you to port this script to other languages.
#  All it does is take in a data object and a message type, and it sends an HTTPS
#  request to the target OMF endpoint
def sendOMFMessageToEndPoint(message_type, OMF_data):
    try:
        # Assemble headers that contain the producer token and message type
        msg_header = {"producertoken": producer_token, "messagetype": message_type, "action": "create", "messageformat": "JSON"}
        # Send the request, and collect the response
        #  -->  response = requests.post(target_url, headers=msg_header, data=json.dumps(OMF_data), verify=verify_SSL, timeout=30)
        # Print a debug message, if desired
        # --> print('Response from relay from the "{0}" message: {1} {2}'.format(message_type, response.status_code, response.text))
    except Exception as e:
            # Log any error, if it occurs
        print((str(datetime.datetime.now())) + " An error occurred during web request: " + str(e))

# ************************************************************************
#  The following function you can customize to allow this script to send along any
#  number of different data values, so long as the values that you send here match
#  up with the values defined in the "DataValuesType" OMF message type (see the next section)
#  In this example, this function simply generates two random values for the sensor values,
#  but here is where you could change this function to reference a library that actually
#  reads from sensors attached to the device that's running the script

def create_data_values_stream_message(target_stream_id, dataFromDevice):
    #  Get the current timestamp
    d = str(datetime.datetime.now())
    timestamp = datetime.datetime.utcnow().isoformat() + 'Z'
    #  Assemble a JSON object containing the stream Id and any data values
    data_values_JSON = [
        {
            "stream": target_stream_id,
            "values": [
                { "Time": timestamp,
                  "Machine Type": dataFromDevice[0],
                  "Platform Type": dataFromDevice[1],
                  "Processor Type": dataFromDevice[2],
                  "CPU Usage": dataFromDevice[3],
                  "Disk Busy Time": dataFromDevice[4],
                  "Memory Used": dataFromDevice[5],
                  "Total Memory": dataFromDevice[6],
                  "Wi-Fi Bits Received": dataFromDevice[7],
                  "Wi-Fi Bits Sent": dataFromDevice[8],
                  "Latitude": dataFromDevice[9],
                  "Longitude": dataFromDevice[10]
                }
            ]
        }
    ]
    return data_values_JSON


# ************************************************************************
def sendDataValueMessage(dataFromDevice):
    # Call the custom function that builds a JSON object that contains new data values see the beginning of this script
    values = create_data_values_stream_message(stream_id_for_sending_values, dataFromDevice)
    # Send the data to the relay it will be stored in a point called <producer_token>.<stream Id>
    sendOMFMessageToEndPoint("data", values)
# ************************************************************************

if __name__ == '__main__':

    sendInitialOMFMessages("5")
    while (1):
        # Loop indefinitely, sending events conforming to the value type that we defined earlier
        time.sleep(3)

        # get the data (from a message or DBMS)

        # Build a data object
        # Include metadata and all analog readings
        dataFromDevice = [
        # Gather general information about the device
            platform.machine(),
            platform.platform(),
            platform.processor(),
        ]

        sendDataValueMessage(dataFromDevice)


