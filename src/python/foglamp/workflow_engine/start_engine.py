
from time import sleep


import asyncio
import psycopg2
import uuid
import engine as eng



async def main():
    print("connecting to the database")

    my_guid = uuid.uuid1()

    try:


        # lets test the new message engine
        store = eng.storage()  # used to add message. this is api

        store.put_message("blah1")
        store.put_message("blah2")


        se = eng.storage_engine("host='localhost' dbname='foglamp' user='foglamp' password='foglamp'", store)




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

