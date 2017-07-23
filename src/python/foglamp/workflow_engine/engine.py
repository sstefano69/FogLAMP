
from threading import Thread
from time import sleep

import threading
from queue import Queue

import asyncio
import psycopg2
import uuid
from enum import IntEnum
from foglamp.storage_layer.storage import storage
from foglamp.storage_layer.storage import messagestate
from foglamp.storage_layer.storage import requesttype




class returncode(IntEnum):
    success=0,
    failed=1,
    success_with_warning=2




class upload_queue:           #just move to read queue
    def __init__(self, conn):
        self.conn=conn

    def process(self, message_id, tostate):  #this is code to actually unpak the message
        cur = self.conn.cursor()
        print("message moved to ready queue "+ str(tostate))
        cur.execute("update message_queue set reader_id=NULL, state = %s  where message_id = %s", ( str(int( tostate)),str(message_id)  ))


class ready_queue:
    def __init__(self, conn):
        self.conn=conn

    def process(self, message_id, tostate):  #this is code to actually unpak the message
        cur = self.conn.cursor()
        cur.execute("select message_queue from message_queue where message_id = %s", [str(message_id)])
        row = cur.fetchone()
        print(row) #for now just print the message
        cur.execute("update message_queue set reader_id=NULL, state = %s where message_id = %s", (str(int(tostate)),str(message_id)))
        print("message moved to next queue")


class decode_queue:
    def __init__(self, conn):
        self.conn=conn

    def process(self, message_id, tostate):  #this is code to actually unpack the message
        cur = self.conn.cursor()
        cur.execute("select message_queue from message_queue where message_id = %s ", [str(message_id)])
        row = cur.fetchone()
        print(row) #for now just print the message
        cur.execute("update message_queue set reader_id=NULL, state = %s  where message_id = %s", (str(int(tostate)), str(message_id)))
        #cur.execute("update message_queue set reader_id=NULL, state = %s  where message_id = %s", (str(int(messagestate.done)),str(message_id)))
        print("message moved to done queue")

class request_response:
    def __init__(self, rc, msg):
        self.return_code = rc
        self.message=msg





class storage_engine:
    def __init__(self, connection_string):
        self.running=True
        self.conn = psycopg2.connect("host='localhost' dbname='foglamp' user='foglamp' password='foglamp'")
        self.conn.set_session(autocommit=True)
        self.reader_id= uuid.uuid1()
        self.rq=ready_queue(self.conn)
        self.uq=upload_queue(self.conn)
        self.dq=decode_queue(self.conn)
        self.q = Queue()


    def get_queue(self):
        return self.q

    def save_message(self, msg):
        cur = self.conn.cursor()
        cur.execute("insert into message_queue (state,reader_id,message_data) values(0, NULL,'blah')")


    def next_message(self):
        cur = self.conn.cursor()
        cur.execute("select message_id from message_queue where reader_id = %s ", [str(self.reader_id)])
        row = cur.fetchone()
        if row is None:
            return 0
        else:
            return row[0]

    def get_state(self, id):
        cur = self.conn.cursor()
        cur.execute("select state from message_queue where message_id = %s ",[str(id)])
        row = cur.fetchone()
        return row[0]

    def get_batch(self):
        cur = self.conn.cursor()
        cur.execute("update message_queue set reader_id= %s where state < %s and reader_id is NULL", ( str(self.reader_id), int( messagestate.done)))

    def create_role(self, request):
        ui = request  #this is useinfo
        #now create the user in the database
        cur = self.conn.cursor()
        ret =cur.execute("insert into roles (name,description) values( %s, %s )",(ui.role,ui.role_description))
        return request_response(returncode.success, 'all good')

    def get_role(self, request):
        ui = request  # this is useinfo
        # now create the user in the database
        cur = self.conn.cursor()
        cur.execute("select id from roles where name = %s", [ui.role])
        row = cur.fetchone()
        ui.role_id=row[0]
        return request_response(returncode.success, ui)

    def delete_roles(self, request):
        #now create the user in the database
        cur = self.conn.cursor()
        ret =cur.execute("delete from roles")
        return request_response(returncode.success, 'all good')

    def delete_users(self, request):
        #now create the user in the database
        cur = self.conn.cursor()
        ret =cur.execute("delete from users")
        return request_response(returncode.success, 'all good')

    def delete_roles_cascade(self, request):
        self.delete_users( request)
        self.delete_roles( request)
        return request_response(returncode.success, 'all good')

    def create_user(self, request):
        ui = request  # this is useinfo
        # now create the user in the database
        cur = self.conn.cursor()
        ret = cur.execute("insert into users (uid,pwd,role_id) values( %s, %s, %s )", (ui.uid, ui.pwd, ui.role_id))
        return request_response(returncode.success, 'all good')




    def process_request(self, reqtype, request):
        if (reqtype == requesttype.ping_engine):
            ret= request_response(returncode.success,'all good')   #just return OK message for now
            return ret

        if (reqtype == requesttype.create_user):
            ret= self.create_user(request)
            return ret

        if (reqtype == requesttype.create_role):
            ret= self.create_role(request)
            return ret

        if (reqtype == requesttype.delete_all_roles):
            ret= self.delete_roles(request)
            return ret

        if (reqtype == requesttype.delete_all_roles_cascade):
            ret= self.delete_roles_cascade(request)
            return ret

        if (reqtype == requesttype.delete_all_users):
            ret= self.delete_users(request)
            return ret

        if (reqtype == requesttype.get_role):
            ret= self.get_role(request)
            return ret

        elif (reqtype == requesttype.stop_engine):
            ret = request_response(returncode.success, 'all good')
            return ret

        elif (reqtype == requesttype.set_config):
            ret = request_response(returncode.success, 'all good')
            return ret

        else:
            pass  # do the default


    def put_request(self,reqtype, request):
        return self.process_request(reqtype,request)


    #The process() function codefies the workflow
    #to add or modify the workflow for a message you should modify this function
    # the current workflow is
    #       (uploaded -> ready -> decode -> done
    # to add an additional step in workflow you add it here
    # note* the second message in the queue process function is the NEXT state
    # for example if you wanted to send the message prior to done you would create a
    # sendQUeue sq, create a new state messagestate.send
    # then change self.dq.process(message_id, messagestate.done) -> self.dq.process(message_id, messagestate.send)
    # now add the worktask
    #  elif (state==messagestate.send):
    #        self.sq.process(message_id, messagestate.done)
    #
    #

    def process(self,message_id):
        state = self.get_state(message_id)
        # all messages start in an uploaded staaaate
        if(state==messagestate.uploaded):
            self.uq.process(message_id, messagestate.ready) # the second argument determiines the next state in workflow
        elif (state==messagestate.ready):
            self.rq.process(message_id, messagestate.decode)
        elif (state==messagestate.decode):
            self.dq.process(message_id, messagestate.done)
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
                    message = self.se.get_queue().get_nowait()
                    self.se.save_message(message)

            except Exception as e:
                pass


            self.se.get_batch()  #grab a batch of mesages for processing


            message_id = self.se.next_message()
            if message_id > 0:
                self.se.process(message_id)

            sleep(1)







