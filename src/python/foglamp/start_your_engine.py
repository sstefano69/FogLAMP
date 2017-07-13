
from time import sleep


import asyncio
import psycopg2
import uuid
import foglamp.workflow_engine.engine as eng
from foglamp.storage_layer.storage import requesttype
from foglamp.storage_layer.storage import userinfo

async def main():
    print("connecting to the database")

    my_guid = uuid.uuid1()

    try:

        #storage_layer (store) is the object where other modules interact with the storage engine
        # for example mesages are added to store by the COAP server
        #the coap server should just add messages to this queue.
        #the storage() has no knowledge about database.
        #these message are ascyncronous (ie. you post and forget)


        #other requests like create_user wil also happen through the storage_layer
        #however these messages requrie the storage engine to be passed in along with the request type
        #these calls are syncronous and will wait for a response.

        # lets test the new message engine
        store = eng.storage()  # used to add message. this is api


        #the storage_engine is the persistent (DB) storage and queue management
        #it has a conection to the database and a reference to the storage queue (store)

        se = eng.storage_engine("host='localhost' dbname='foglamp' user='foglamp' password='foglamp'", store)

        store.put_message("blah1")   #add mesage to queue, workflow engine will process
        store.put_message("blah2")

        # test some requests
        ret1 = store.put_request(se, requesttype.ping_engine, 'blah')
        print("result="+ret1.message)

        ret = store.put_request(se, requesttype.delete_all_roles_cascade, None)


        ui = userinfo()

        ui.set_role("standard_user", "this is a standard user")


        print("create role")
        ret2 = store.put_request(se, requesttype.create_role, ui)

        ret = store.put_request(se, requesttype.get_role, ui)
        new_row_id = ret.message.role_id


        ui.set_user('mike','$55',new_row_id)

        print("create user")
        ret3 = store.put_request(se,requesttype.create_user,ui)

        wt = eng.worker_thread(se)
        wt.start()


        #test some requests
        ret1 = store.put_request(se, requesttype.ping_engine, 'blah')
        print("result=" + ret1.message)

        ret2 = store.put_request(se, requesttype.stop_engine, 'blah')
        print("result=" + ret2.message)





        sleep(10)  # sleep for a while
        print("Stopping queue engine")
        wt.running = False

        wt.join()  # wait for thread to end



    except Exception as e:
        print("Error: " + str(e))


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())

print("all Done.");

