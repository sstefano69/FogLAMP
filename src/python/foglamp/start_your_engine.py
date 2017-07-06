
from time import sleep


import asyncio
import psycopg2
import uuid
import foglamp.workflow_engine.engine as eng
from foglamp.storage_layer.storage import requesttype


async def main():
    print("connecting to the database")

    my_guid = uuid.uuid1()

    try:

        #storage_layer (store) is the object where other modules interact with the storage engine
        # for example mesages are added to store by the COAP server
        #the coap server should just add messages to this queue.
        #the storage() has no knowlage about and database.
        #these message are ascyncronous (ie. you post and forget)


        #other requests like create_user wil also happen through the storage_layer
        #however these messages will wait for a response.

        # lets test the new message engine
        store = eng.storage()  # used to add message. this is api

        store.put_message("blah1")
        store.put_message("blah2")


        #the storage_engine is the persistent (DB) storage and queue management
        #it has a conection to the database and a reference to the storage queue (store)

        se = eng.storage_engine("host='localhost' dbname='foglamp' user='foglamp' password='foglamp'", store)


        ret1 = store.put_request(se, requesttype.ping_engine, 'blah')

        wt = eng.worker_thread(se)
        wt.start()


        #test some requests
        ret=se.put_request(eng.requesttype.ping_engine, "hello")
        print("result="+ret.message)

        se.put_request(eng.requesttype.stop_engine, "hello")



        sleep(10)  # sleep for a while
        print("Stopping queue engine")
        wt.running = False

        wt.join()  # wait for thread to end



    except Exception as e:
        print("Error: " + str(e))


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())

print("all Done.");

