#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2017

"""  backup

"""

import time
import psycopg2
import sys

from foglamp import logger

import foglamp.backup_restore.lib as lib


# FIXME: it will be removed using the DB layer
_DB_CONNECTION_STRING = "user='foglamp' dbname='foglamp'"

_MODULE_NAME = "foglamp_backup"

class BackupError(RuntimeError):
    """ # FIXME: """
    pass


# noinspection PyProtectedMember
def log_backup(file_name, backup_exit_status):
    """" # FIXME: """

    _logger.debug("{0} - file name |{1}| ".format(sys._getframe().f_code.co_name, file_name))

    _pg_conn = psycopg2.connect(_DB_CONNECTION_STRING)
    _pg_cur = _pg_conn.cursor()

    sql_cmd = """
        INSERT INTO foglamp.backups
        (file_name, ts, type, status)
        VALUES ('{file}', now(), 0, {status} );
        """.format(file=file_name,
                   status=backup_exit_status)

    _pg_cur.execute(sql_cmd)
    _pg_conn.commit()
    _pg_conn.close()


# noinspection PyProtectedMember
def update_backup_status(file_name, backup_exit_status):
    """" # FIXME: """

    _logger.debug("{0} - file name |{1}| ".format(sys._getframe().f_code.co_name, file_name))

    _pg_conn = psycopg2.connect(_DB_CONNECTION_STRING)
    _pg_cur = _pg_conn.cursor()

    sql_cmd = """

        UPDATE foglamp.backups SET  status={status} WHERE file_name='{file}';

        """.format(status=backup_exit_status,
                   file=file_name,)

    _pg_cur.execute(sql_cmd)
    _pg_conn.commit()
    _pg_conn.close()


# noinspection PyProtectedMember
def exec_backup(_backup_file):
    """" # FIXME: """

    _logger.debug("{0} - ".format(sys._getframe().f_code.co_name))

    # Evaluates the parameters
    database = "foglamp"
    host = "localhost"
    port = 5432

    # Executes the backup
    cmd = "pg_dump"
    cmd += " --serializable-deferrable -Fc  "
    cmd += " -h {host} -p {port} {db} > {file}".format(
        host=host,
        port=port,
        db=database,
        file=_backup_file)

    _status, output = lib.exec_wait_retry(cmd, True, 0)

    _logger.debug("{func} - _status |{status}|  output |{output}| ".format(
                func=sys._getframe().f_code.co_name,
                status=_status,
                output=output))


    if _status != 0:
        # FIXME:
        raise BackupError

    _logger.debug("{func} - END".format(func=sys._getframe().f_code.co_name))

    return _status


# noinspection PyProtectedMember
def generate_file_name():
    """" # FIXME: """

    _logger.debug("{0} - ".format(sys._getframe().f_code.co_name))

    # Evaluates the parameters
    database = "foglamp"

    backup_dir = "/tmp"

    execution_time = time.strftime("%Y_%m_%d_%H_%M_%S")

    full_file_name = backup_dir + "/" + "foglamp" + "_" + execution_time
    ext = "dump"

    _backup_file = "{file}.{ext}".format(file=full_file_name, ext=ext)

    return _backup_file


if __name__ == "__main__":

    try:
        _logger = logger.setup(_MODULE_NAME)

    except Exception as ex:
        message = ex
        _logger.debug("{func} - ERROR  |{err}| ".format(func=sys._getframe().f_code.co_name,
                                                        err=message))

        sys.exit(1)
    else:

        try:
            backup_file = generate_file_name()
            log_backup(backup_file, -1)
            status = exec_backup(backup_file)
            update_backup_status(backup_file, status)

            sys.exit(0)

        except Exception as ex:
            message = ex
            _logger.debug("{func} - ERROR  |{err}| ".format(func=sys._getframe().f_code.co_name,
                                                            err=message))

            sys.exit(1)
