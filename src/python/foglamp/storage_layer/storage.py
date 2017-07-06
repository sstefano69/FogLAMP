
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

class storage(object):

    def __init__(self):
        self.q = Queue()


    def get_queue(self):
        return self.q


    def put_message(self, msg):
        self.q.put(msg)




    def put_request(self, eng, reqt, req):
        return eng.put_request(reqt,req)