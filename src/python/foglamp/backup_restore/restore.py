#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2017

"""  backup

"""

# FIXME:
import argparse
import time
import sys

from foglamp import logger
import foglamp.backup_restore.lib as lib


_MODULE_NAME = "foglamp_restore"

_MESSAGES_LIST = {

    # Information messages
    "i000001": "Started.",
    "i000002": "Execution completed.",

    # Warning / Error messages
    "e000000": "general error",
    "e000001": "Invalid file name",
}
""" Messages used for Information, Warning and Error notice """

# FIXME:
base_cmd = "bash -c '"
base_cmd += "source /home/foglamp/Development/FogLAMP/src/python/venv/foglamp/bin/activate;\\"
base_cmd += "python3 -m foglamp {0}"
base_cmd += "'"

# base_cmd = "python3 -m foglamp {0}"

STATUS_NOT_DEFINED = 0
STATUS_STOPPED = 1
STATUS_RUNNING = 2

_logger = ""


class RestoreError(RuntimeError):
    """ # FIXME: """
    pass


class NoBackupAvailableError(RuntimeError):
    """ # FIXME: """
    pass


class InvalidFileNameError(RuntimeError):
    """ # FIXME: """
    pass


class FileNameError(RuntimeError):
    """ # FIXME: """
    pass


class FogLAMPStartError(RuntimeError):
    """ # FIXME: """
    pass


class FogLAMPStopError(RuntimeError):
    """ # FIXME: """
    pass


# noinspection PyProtectedMember
def foglamp_stop():
    """" # FIXME: """

    cmd = base_cmd.format("stop")

    # Restore the backup
    # FIXME:
    status, output = lib.exec_wait_retry(cmd, True)

    _logger.debug("FogLAMP {func} - cmd |{cmd}| - status |{status}| - output |{output}|   ".format(
                func=sys._getframe().f_code.co_name,
                cmd=cmd,
                status=status,
                output=output))

    if status == 0:
        if foglamp_status() == STATUS_STOPPED:
            # FIXME:
            cmd = "pkill -9  -f 'python3 -m foglamp.device'"
            status, output = lib.exec_wait(cmd, True)

            _logger.debug("FogLAMP pkill {0} - output |{1}| -  status |{2}|  ".format(
                                                                        sys._getframe().f_code.co_name,
                                                                        output,
                                                                        status))

        else:
            raise FogLAMPStopError
    else:
        raise FogLAMPStopError


# noinspection PyProtectedMember
def foglamp_start():
    """" # FIXME: """

    cmd = base_cmd.format("start")

    status, output = lib.exec_wait_retry(cmd, True)

    _logger.debug("FogLAMP {0} - output |{1}| -  status |{2}|  ".format(sys._getframe().f_code.co_name,
                                                                        output,
                                                                        status))

    if status == 0:
        if foglamp_status() != STATUS_RUNNING:
            raise FogLAMPStartError

    else:
        raise FogLAMPStartError


# noinspection PyProtectedMember
def foglamp_status():
    """" # FIXME: """

    status = STATUS_NOT_DEFINED

    num_exec = 1
    max_exec = 20
    same_status = 1
    same_status_ok = 3
    sleep_time = 1

    while (same_status <= same_status_ok) and (num_exec <= max_exec):

        time.sleep(sleep_time)

        try:
            cmd = base_cmd.format("status")

            cmd_status, output = lib.exec_wait(cmd, True)

            _logger.debug("{0} - output |{1}| \r - status |{2}|  ".format(sys._getframe().f_code.co_name,
                                                                          output,
                                                                          cmd_status))

            num_exec += 1

            if cmd_status == 0:
                new_status = STATUS_RUNNING

            elif cmd_status == 2:
                new_status = STATUS_STOPPED

        except Exception as e:
            _message = e
            raise _message

        else:
            if same_status == 1:
                same_status += 1

            else:
                if new_status == status:
                    same_status += 1

            status = new_status

    if num_exec >= max_exec:
        _logger.error("ERROR - Max exec reached")
        status = STATUS_NOT_DEFINED

    return status


# noinspection PyProtectedMember
def exec_restore(backup_file):
    """" # FIXME: """

    # Evaluates the parameters

    _logger.debug("{func} - Restore start |{file}|".format(func=sys._getframe().f_code.co_name,
                                                           file=backup_file))

    database = "foglamp"
    host = "localhost"
    port = 5432

    # Generates the restore command
    cmd = "pg_restore"
    cmd += " --verbose --clean --no-acl --no-owner "
    cmd += " -h {host} -p {port} -d {db} {file}".format(
        host=host,
        port=port,
        db=database,
        file=backup_file,)

    # Restore the backup
    # FIXME:
    status, output = lib.exec_wait_retry(cmd, True)
    output_short = output.splitlines()[10]

    _logger.debug("{func} - Restore end - status |{status}| - output |{output}|".format(
                                func=sys._getframe().f_code.co_name,
                                status=status,
                                output=output_short))

    if status != 0:
        raise RestoreError


# noinspection PyProtectedMember
# noinspection PyUnresolvedReferences
def retrieve_last_backup():
    """" # FIXME: """

    _logger.debug("{0} ".format(sys._getframe().f_code.co_name))

    sql_cmd = """
        SELECT file_name FROM foglamp.backups WHERE (ts,id)=
        (SELECT  max(ts),MAX(id) FROM foglamp.backups WHERE status=0 or status=-2);
    """

    data = lib.storage_retrieve(sql_cmd)

    if len(data) == 0:
        raise NoBackupAvailableError

    elif len(data) == 1:
        _file_name = data[0]['file_name']
    else:
        raise FileNameError

    return _file_name


# noinspection PyProtectedMember
def update_backup_status(_file_name, exit_status):
    """" # FIXME: """

    _logger.debug("{0} - file name |{1}| ".format(sys._getframe().f_code.co_name, _file_name))

    sql_cmd = """

        UPDATE foglamp.backups SET  status={status} WHERE file_name='{file}';

        """.format(status=exit_status,
                   file=_file_name, )

    lib.storage_update(sql_cmd)


def handling_input_parameters():
    """ Handles command line parameters

    Raises :

    """

    parser = argparse.ArgumentParser(prog=_MODULE_NAME)
    parser.description = '%(prog)s -- restore a fogLAMP backup '

    parser.epilog = ' '

    parser.add_argument('-f', '--file_name',
                        required=False,
                        default=0,
                        help='Backup file to restore.')

    namespace = parser.parse_args(sys.argv[1:])

    try:
        _file_name = namespace.file_name if namespace.file_name else None

    except Exception:
        _message = _MESSAGES_LIST["e000001"].format(str(sys.argv))

        _logger.error(_message)
        raise InvalidFileNameError(_message)

    return _file_name

if __name__ == "__main__":

    try:
        _logger = logger.setup(_MODULE_NAME)

        # Set the logger for the library
        lib._logger = _logger

    except Exception as ex:
        message = ex
        print("ERROR  |{err}| ".format(err=message))
        sys.exit(1)

    else:

        try:
            # Checks if a file name is provided as command line parameter, if not it considers latest backup
            file_name = handling_input_parameters()

            if not file_name:
                file_name = retrieve_last_backup()

            foglamp_stop()

            try:
                exec_restore(file_name)
                update_backup_status(file_name, -2)

            except Exception as ex:
                message = ex
                _logger.error("error details |{0}|  ".format(message))

            finally:
                try:
                    foglamp_start()
                    _logger.info("RESTORE COMPLETED ")

                except Exception as ex:
                    message = ex
                    _logger.error("restarting foglamp |{0}|  ".format(message))

        except Exception as ex:
            message = ex
            _logger.error("error details |{0}|  ".format(message))
