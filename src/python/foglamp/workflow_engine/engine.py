
from threading import Thread
from time import sleep

import threading
from queue import Queue

import asyncio
import psycopg2
import uuid
from enum import IntEnum


class messagestate(IntEnum):
    uploaded=0
    ready=1
    done=2

class requesttype(IntEnum):
    ping_engine=0
    stop_engine=1
    set_config=2
    get_config=3
    create_user=4
    drop_user=5
    get_reading=6

class returncode(IntEnum):
    success=0,
    failed=1,
    success_with_warning=2


class storage(object):

    def __init__(self):
        self.q = Queue()

    def get_queue(self):
        return self.q


    def put_message(self, msg):
        self.q.put(msg)




class upload_queue:           #just move to read queue
    def __init__(self, conn):
        self.conn=conn

    def process(self, message_id):  #this is code to actually unpak the message
        cur = self.conn.cursor()
        print("message moved to ready queue "+ str(int( messagestate.ready)))
        cur.execute("update message_queue set reader_id=NULL, state = "+ str(int( messagestate.ready)) + " where message_id ='" + str(message_id) + "'")


class ready_queue:
    def __init__(self, conn):
        self.conn=conn

    def process(self, message_id):  #this is code to actually unpak the message
        cur = self.conn.cursor()
        cur.execute("select message_queue from message_queue where message_id ='" + str(message_id) + "'")
        row = cur.fetchone()
        print(row) #for now just print the message
        cur.execute("update message_queue set reader_id=NULL, state = "+str(int(messagestate.done)) + " where message_id ='" + str(message_id) + "'")
        print("message moved to done queue")




class request_response:
    def __init__(self, rc, msg):
        self.return_code = rc
        self.message=msg





class storage_engine:
    def __init__(self, connection_string, stor):
        self.running=True
        self.conn = psycopg2.connect("host='localhost' dbname='foglamp' user='foglamp' password='foglamp'")
        self.conn.set_session(autocommit=True)
        self.reader_id= uuid.uuid1()
        self.store=stor
        self.rq=ready_queue(self.conn)
        self.uq=upload_queue(self.conn)

    def save_message(self, msg):
        cur = self.conn.cursor()
        cur.execute("insert into message_queue (state,reader_id,message_data) values(0, NULL,'blah')")


    def next_message(self):
        cur = self.conn.cursor()
        cur.execute("select message_id from message_queue where reader_id ='" + str(self.reader_id) + "'")
        row = cur.fetchone()
        if row is None:
            return 0
        else:
            return row[0]

    def get_state(self, id):
        cur = self.conn.cursor()
        cur.execute("select state from message_queue where message_id ='" + str(id) + "'")
        row = cur.fetchone()
        return row[0]

    def get_batch(self):
        cur = self.conn.cursor()
        cur.execute("update message_queue set reader_id='" + str(self.reader_id) + "' where state <"+ str(int( messagestate.done)) + " and reader_id is NULL")



    def process_request(self, reqtype, request):
        if (reqtype == requesttype.ping_engine):
            ret= request_response(returncode.success,'all good')   #just return OK message for now
            return ret
        elif (reqtype == requesttype.stop_engine):
            pass
        elif (reqtype == requesttype.set_config):
            pass
        else:
            pass  # do the default


    def put_request(self,reqtype, request):
        return self.process_request(reqtype,request)




    def process(self,message_id):
        state = self.get_state(message_id)

        if(state==messagestate.uploaded):
            self.uq.process(message_id)
        elif (state==messagestate.ready):
            self.rq.process(message_id)
        elif (state==messagestate.done):
            pass
        else:
            pass #do the default


class worker_thread(threading.Thread):
    def __init__(self, se):
        threading.Thread.__init__(self)
        self.se = se
        self.running=True


    def run(self):
        print ("Starting worker thread")
        while self.running:  #thread will loop until stopped

            # first lets insert all messages added to storage to the db message_queue
            try:
                while True:
                    message = self.se.store.get_queue().get_nowait()
                    self.se.save_message(message)

            except Exception as e:
                pass


            self.se.get_batch()  #grab a batch of mesages for processing


            message_id = self.se.next_message()
            if message_id > 0:
                self.se.process(message_id)

            sleep(1)







