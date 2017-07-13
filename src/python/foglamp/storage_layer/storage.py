
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
    delete_user=5
    get_reading=6
    delete_role=7,
    delete_all_roles=8,
    get_role=9,
    delete_all_roles_cascade =10  #will remove all usesrs as well
    delete_all_users=11,
    create_role=12


class userinfo(object):

    def __init__(self):
        self.uid=""
        self.pwd=""
        self.role=""
        self.role_id=0
        self.role_description=""

    def set_role(self, _role, _role_desc):
        self.role=_role
        self.role_description=_role_desc

    def set_user(self, _uid, _pwd, _role_id):
        self.uid=_uid
        self.pwd=_pwd
        self.role_id=_role_id










class storage(object):

    def __init__(self):
        self.q = Queue()


    def get_queue(self):
        return self.q


    def put_message(self, msg):
        self.q.put(msg)




    def put_request(self, eng, reqt, req):
        return eng.put_request(reqt,req)