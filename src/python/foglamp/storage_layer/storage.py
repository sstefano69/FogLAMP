
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



class storage(object):

    def __init__(self):
        self.q = Queue()


    def get_queue(self):
        return self.q


    def put_message(self, msg):
        self.q.put(msg)

