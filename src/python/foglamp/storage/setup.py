
from threading import Thread
from time import sleep

import queue

import asyncio
import psycopg2
import uuid
from enum import Enum


class messagestate(Enum):
    uploaded=0
    ready=1
    done=2


class storage(object):

    def __init__(self):
        self.q = queue()

    def get_queue(self):
        return self.q


    def put_message(self, msg):
        self.q.put(msg)


class storage_engine:
    def __init__(self, connection_string, stor):
        self.running=True
        self.conn = psycopg2.connect("host='localhost' dbname='foglamp' user='foglamp' password='foglamp'")
        self.reader_id= uuid.uuid1()
        self.store=stor

    def save_message(self, msg):
        cur = self.conn.cursor()
        cur.execute("insert into message_queue (state,reader_id,message_data) values(0,NULL,'blah')")


    def next_message(self):
        cur = self.conn.cursor()
        cur.execute("select message_id from message_queue where reader_id ='" + str(self.reader_id) + "'")


    def get_state(self, id):
        cur = self.conn.cursor()
        cur.execute("select state from message_queue where message_id ='" + str(id) + "'")
        row = cur.fetchone()
        return row[0]



    def process(self,id):
        state= get_state(id)
        





def worker_thread(se):
    while True:

        #first lets insert all messages added to storage to the db message_queue
        try:
            while True:
                message=se.store.get_queue().get()
                se.save_message(message)

        except:
            pass


        message_id= se.next_message()
        if message_id>0:
            se.process(message_id)




class threadData(object):
    def __init__(self, connect_string):
        self.conn = psycopg2.connect(connect_string)
        self.running = True

def threaded_function(arg):


     while(arg.running):
        print("running")
        sleep(1)



async def main():






        print("connecting to the database")

        my_guid=uuid.uuid1()

        try:
            conn = psycopg2.connect("host='localhost' dbname='foglamp' user='foglamp' password='foglamp'")
            print("i have connected")

            #now create table

            cur = conn.cursor()
            try:
                cur.execute("drop table message_queue")
            except:
                conn.rollback()
                print("ignore drop")

            cur.execute("create table message_queue (message_id  serial primary key, state int, reader_id uuid, message_data bytea, create_time timestamp(6) with time zone NOT NULL DEFAULT now())")
            print("table created")

            # lets insert a message
            for i in range(0,10):
                cur.execute("insert into message_queue (state,reader_id,message_data) values(0,NULL,'blah')")

            conn.commit()

            # now mark rows that can get processed.
            # single thread will select all rows which are in a ready state
            cur.execute("update message_queue set reader_id='"+str(my_guid)+"' where state=0 and reader_id is NULL")


            conn.commit()


            # now grab the rows that belong to me
            cur.execute("select message_id,state from message_queue where reader_id ='"+str(my_guid)+"'" )
            for row in cur:
                print("row: "+ str(row[0]))





        except Exception as e:
            print("Error: "+str(e))





if __name__ == "__main__":

    asyncio.get_event_loop().run_until_complete(main())











print("all Done.");

