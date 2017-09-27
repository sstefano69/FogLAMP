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
import os

from foglamp import logger


_MESSAGES_LIST = {

    # Information messages
    "i000000": "Information",

    # Warning / Error messages
    "e000000": "general error",
    "e000001": "semaphore file deleted because it was already in existence - file |{0}|",
    "e000002": "semaphore file deleted because it existed even if the corresponding process was not running "
               "- file |{0}| - pid |{1}|",
}
""" Messages used for Information, Warning and Error notice """


# FIXME: it will be removed using the DB layer
_DB_CONNECTION_STRING = "user='foglamp' dbname='foglamp'"

_CMD_TIMEOUT = " timeout --signal=9  "

BACKUP_STATUS_SUCCESSFUL = 0
BACKUP_STATUS_RUNNING = -1
BACKUP_STATUS_RESTORED = -2

# FIXME:
# JOB_SEM_FILE_PATH = "/home/foglamp/Development/FogLAMP/src/python/foglamp/backup_restore"
JOB_SEM_FILE_PATH = "/tmp"
JOB_SEM_FILE_BACKUP = "backup.sem"
JOB_SEM_FILE_RESTORE = "restore.sem"

_logger = ""


# noinspection PyProtectedMember
def storage_update(sql_cmd):
    """  Executes a sql command against the Storage layer that updates data

    Args:
        sql_cmd: sql command to execute
    Returns:
    Raises:
    Todo:
    """

    _logger.debug("{func} - sql cmd |{cmd}| ".format(func=sys._getframe().f_code.co_name,
                                                     cmd=sql_cmd))

    _pg_conn = psycopg2.connect(_DB_CONNECTION_STRING)
    _pg_cur = _pg_conn.cursor()

    _pg_cur.execute(sql_cmd)
    _pg_conn.commit()
    _pg_conn.close()


# noinspection PyProtectedMember
def storage_retrieve(sql_cmd):
    """  Executes a sql command against the Storage layer that retrieves data

    Args:
    Returns:
        raw_data: Python list containing the rows retrieved from the Storage layer
    Raises:
    Todo:
    """

    _logger.debug("{func} - sql cmd |{cmd}| ".format(func=sys._getframe().f_code.co_name,
                                                     cmd=sql_cmd))

    _pg_conn = psycopg2.connect(_DB_CONNECTION_STRING, cursor_factory=RealDictCursor)

    _pg_cur = _pg_conn.cursor()

    _pg_cur.execute(sql_cmd)
    raw_data = _pg_cur.fetchall()

    return raw_data


# noinspection PyProtectedMember
def exec_wait(_cmd, output_capture=False, timeout=0):
    """  Executes an external/shell commands

    Args:
        _cmd: command to execute
        output_capture: if the output of the command should be captured or not
        timeout: 0 no timeout or the timeout in seconds for the execution of the command

    Returns:
        status: exit status of the command
        output: output of the command
    Raises:
    Todo:
    """

    _output = ""

    if timeout != 0:
        _cmd = _CMD_TIMEOUT + str(timeout) + " " + _cmd
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
    """ Executes an external command retrying x time the operation up to the exit status match a specific value

    Args:
        cmd: command to execute
        output_capture: if the output of the command should be captured or not
        status_ok: exit status to achieve
        max_retry: maximum number of retries to achieve the desired exit status
        write_error: if a message should be generate for each retry
        sleep_time: seconds to sleep between each retry
        timeout: 0 no timeout or the timeout in seconds for the execution of the command

    Returns:
        status: exit status of the command
        output: output of the command

    Raises:
    Todo:
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


# noinspection PyProtectedMember
def backup_status_create(file_name, status):
    """ Logs the creation of the backup in the Storage layer

    Args:
        file_name: file_name, as a full path, used as if of the backup
        status: BACKUP_STATUS_RUNNING
    Returns:
    Raises:
    Todo:
    """

    _logger.debug("{0} - file name |{1}| ".format(sys._getframe().f_code.co_name, file_name))

    sql_cmd = """
        INSERT INTO foglamp.backups
        (file_name, ts, type, status)
        VALUES ('{file}', now(), 0, {status} );
        """.format(file=file_name,
                   status=status)

    storage_update(sql_cmd)


# noinspection PyProtectedMember
def backup_status_update(file_name, status):
    """ Update the status of the backup in the Storage layer

    Args:
        file_name: file_name, as a full path, used as if of the backup
        status: {exit status of the backup|BACKUP_STATUS_RESTORED|}
    Returns:
    Raises:
    Todo:
    """

    _logger.debug("{0} - file name |{1}| ".format(sys._getframe().f_code.co_name, file_name))

    sql_cmd = """

        UPDATE foglamp.backups SET  status={status} WHERE file_name='{file}';

        """.format(status=status,
                   file=file_name, )

    storage_update(sql_cmd)


class Job:
    """" Handles backup and restore processes synchronization """

    @classmethod
    def _pid_file_retrieve(cls, file_name):
        """ Retrieve the PID from  the semaphore file

        Args:
        Returns:
            pid: 0= another process is in execution or the pid retrieved from the semaphore file
        Raises:
        Todo:
        """

        with open(file_name) as f:
            pid = f.read()

        pid = int(pid)

        return pid

    @classmethod
    def _pid_file_create(cls, file_name, pid):
        """ Creates the semaphore file having the PID as content

        Args:
            file_name:
            pid: stored into the file
        Returns:
        Raises:
        Todo:
        """

        file = open(file_name, "w")
        file.write(str(pid))
        file.close()

    @classmethod
    def check_semaphore_file(cls, file_name):
        """ Evaluates if another either backup or restore job is already running

        Args:
        Returns:
            pid: 0= another process is in execution or the pid retrieved from the semaphore file
        Raises:
        Todo:
        """

        _logger.debug("{0}".format(sys._getframe().f_code.co_name))

        pid = 0

        if os.path.exists(file_name):
            pid = cls._pid_file_retrieve(file_name)

            # Check if the process is really running
            try:
                os.getpgid(pid)
            except ProcessLookupError:
                # Process is not running, removing the semaphore file
                os.remove(file_name)
                pid = 0

                message = _MESSAGES_LIST["e000002"].format(file_name, pid)
                _logger.warning("{0}".format(message))

        return pid

    @classmethod
    def is_running(cls):
        """ Evaluates if another either backup or restore job is already running

        Args:
        Returns:
            pid: 0= another process is in execution or the pid retrieved from the semaphore file
        Raises:
        Todo:
        """

        _logger.debug("{0}".format(sys._getframe().f_code.co_name))

        # Checks if a backup process is still running
        full_path_backup = JOB_SEM_FILE_PATH + "/" + JOB_SEM_FILE_BACKUP
        pid = cls.check_semaphore_file(full_path_backup)

        # Checks if a restore process is still running
        if pid == 0:
            full_path_restore = JOB_SEM_FILE_PATH + "/" + JOB_SEM_FILE_RESTORE
            pid = cls.check_semaphore_file(full_path_restore)

        return pid

    @classmethod
    def set_as_running(cls, file_name, pid):
        """ Set a job as running

        Args:
            file_name: semaphore file either fot backup or restore
            pid: pid of the process stored within the semaphore
        Returns:
        Raises:
        Todo:
        """

        _logger.debug("{0}".format(sys._getframe().f_code.co_name))

        full_path = JOB_SEM_FILE_PATH + "/" + file_name

        if os.path.exists(full_path):

            os.remove(full_path)

            message = _MESSAGES_LIST["e000001"].format(full_path)
            _logger.warning("{0}".format(message))

        cls._pid_file_create(full_path, pid)

    @classmethod
    def set_as_completed(cls, file_name):
        """ Set a job as completed

        Args:
            file_name: semaphore file either fot backup or restore

        Returns:
        Raises:
        Todo:
        """

        _logger.debug("{0}".format(sys._getframe().f_code.co_name))

        full_path = JOB_SEM_FILE_PATH + "/" + file_name

        if os.path.exists(full_path):
            os.remove(full_path)


if __name__ == "__main__":

    _logger = logger.setup(__name__)
