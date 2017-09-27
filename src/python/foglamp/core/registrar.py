# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""FogLAMP Registrar module"""

import asyncio
import aiohttp
import collections
import datetime
import logging
import math
import time
import uuid
from enum import IntEnum
from typing import Iterable, List, Tuple, Union
import os
import json

import aiopg.sa
import sqlalchemy
from sqlalchemy.dialects import postgresql as pg_types

from foglamp import logger
from foglamp import configuration_manager
from foglamp.core.service_registry.instance import Service

__author__ = "Ashwin Gopalakrishnan"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

# Module attributes
_CONNECTION_STRING = "dbname='foglamp' user='foglamp'"

try:
  snap_user_common = os.environ['SNAP_USER_COMMON']
  unix_socket_dir = "{}/tmp/".format(snap_user_common)
  _CONNECTION_STRING = _CONNECTION_STRING + " host='" + unix_socket_dir + "'"
except KeyError:
  pass


class Registrar(object):
    _logger = None  # type: logging.Logger


    def __init__(self):
        """Constructor"""

        cls = Registrar

        # Initialize class attributes
        if not cls._logger:
            cls._logger = logger.setup(__name__)
            # cls._logger = logger.setup(__name__, destination=logger.CONSOLE, level=logging.DEBUG)
            # cls._logger = logger.setup(__name__, level=logging.DEBUG)

        self._registrar_loop_task = None  # type: asyncio.Task

    async def _registrar_loop(self):
        """Main loop for the scheduler"""
        # check health of all microservices every N seconds

        while True:
            print("Hey I'm here")
            # print(Service.Instances.all())
            # get all services:
            all_services = Service.Instances.all()
            for service in all_services:
                url = "{}://{}:{}/foglamp/service/ping".format(service._protocol, service._address, service._port)
                print(url)
                async with aiohttp.ClientSession() as session:
                    try:
                        async with session.get(url, timeout=3) as resp:
                            text = await resp.text()
                            res = json.loads(text)
                            if res["uptime"] is None:
                                raise ValueError('Improper Response')
                    except:
                        service._status = 0
                        print("no")
                    else:
                        print("yes")
                        service._status = 1
            await asyncio.ensure_future(asyncio.sleep(5))

    async def start(self):
        self._scheduler_loop_task = asyncio.ensure_future(self._registrar_loop())
