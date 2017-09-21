#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2017

"""  backup

"""

import subprocess
import sys
import time

from foglamp import logger

_logger = ""


def exec_wait(_cmd, output_capture=False):
    """ FIXME """

    _output = ""

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


#
# Executes an external command : it retries the operation x times up to the
#
# noinspection PyProtectedMember
def exec_wait_retry(cmd, output_capture=False, status_ok=0, max_retry=3,  write_error=True, sleep_time=1):
    """ # FIXME: """

    global _logger

    _status = 0
    output = ""

    # exec N times the copy operation
    retry = 1
    loop_continue = True

    while loop_continue:

        _status, output = exec_wait(cmd, output_capture)

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
