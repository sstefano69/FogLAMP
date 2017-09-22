#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2017

"""  backup

"""

import subprocess
import sys
import time
import psycopg2
from psycopg2.extras import RealDictCursor

from foglamp import logger

_logger = ""

# FIXME: it will be removed using the DB layer
_DB_CONNECTION_STRING = "user='foglamp' dbname='foglamp'"

CMD_TIMEOUT = " timeout --signal=9  "


# noinspection PyProtectedMember
def storage_update(sql_cmd):
    """" # FIXME: """

    _logger.debug("{func} - sql cmd |{cmd}| ".format(func=sys._getframe().f_code.co_name,
                                                     cmd=sql_cmd))

    _pg_conn = psycopg2.connect(_DB_CONNECTION_STRING)
    _pg_cur = _pg_conn.cursor()

    _pg_cur.execute(sql_cmd)
    _pg_conn.commit()
    _pg_conn.close()


# noinspection PyProtectedMember
def storage_retrieve(sql_cmd):
    """" # FIXME: """

    _logger.debug("{func} - sql cmd |{cmd}| ".format(func=sys._getframe().f_code.co_name,
                                                     cmd=sql_cmd))

    _pg_conn = psycopg2.connect(_DB_CONNECTION_STRING, cursor_factory=RealDictCursor)

    _pg_cur = _pg_conn.cursor()

    _pg_cur.execute(sql_cmd)
    raw_data = _pg_cur.fetchall()

    return raw_data


# noinspection PyProtectedMember
def exec_wait(_cmd, output_capture=False, timeout=0):
    """ FIXME """

    _output = ""

    if timeout != 0:
        _cmd = CMD_TIMEOUT + str(timeout) + " " + _cmd
        _logger.debug("Executing command using the timeout |{timeout}| ".format(timeout=timeout))

    _logger.debug("{func} - cmd |{cmd}| ".format(func=sys._getframe().f_code.co_name,
                                                 cmd=_cmd))

    if output_capture:
        process = subprocess.Popen(_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    else:
        process = subprocess.Popen(_cmd, shell=True)

    _status = process.wait()

    if output_capture:
        _output = process.stdout.read()

    new_output = _output.decode("utf-8")
    new_output2 = new_output.replace("\n", "\n\r")

    return _status, new_output2


# noinspection PyProtectedMember
def exec_wait_retry(cmd, output_capture=False, status_ok=0, max_retry=3,  write_error=True, sleep_time=1, timeout=0):
    """ # FIXME:
    Executes an external command : it retries the operation x times up to the

    """

    global _logger

    _logger.debug("{func} - cmd |{cmd}| ".format(func=sys._getframe().f_code.co_name,
                                                 cmd=cmd))

    _status = 0
    output = ""

    # exec N times the copy operation
    retry = 1
    loop_continue = True

    while loop_continue:

        _status, output = exec_wait(cmd, output_capture, timeout)

        if _status == status_ok:
            loop_continue = False

        elif retry <= max_retry:

            if write_error:
                short_output = output[0:50]
                _logger.debug("{func} - cmd |{cmd}| - N retry |{retry}| - message |{msg}| ".format(
                    func=sys._getframe().f_code.co_name,
                    cmd=cmd,
                    retry=retry,
                    msg=short_output)
                )

            time.sleep(sleep_time)
            retry += 1

        else:
            loop_continue = False

    return _status, output

if __name__ == "__main__":

    _logger = logger.setup(__name__)
