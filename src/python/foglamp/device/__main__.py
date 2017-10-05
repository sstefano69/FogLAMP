#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

import sys
import argparse

import foglamp.device.exceptions as exceptions
from foglamp import logger
from foglamp.device.server import Server

"""Starts the device server"""

__author__ = "Terris Linenbach"
__copyright_ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_MODULE_NAME = "device_service"

_MESSAGES_LIST = {

    # Information messages
    "i000000": "",

    # Warning / Error messages
    "e000000": "generic error.",
    "e000001": "cannot proceed the execution, invalid device service name - service name |{0}|  error details |{1}|",
}
""" Messages used for Information, Warning and Error notice """

_logger = logger.setup(_MODULE_NAME)


def _handling_input_parameters():
    """ Handles command line parameters

    Args:
    Returns:
    Raises:
        InvalidServiceNameError
    Todo:
    """

    parser = argparse.ArgumentParser(prog=_MODULE_NAME)
    parser.description = '%(prog)s -- Device service for handling operations related to input devices.'

    parser.epilog = ' '

    parser.add_argument('-n', '--name',
                        required=False,
                        default=0,
                        help='Device service name')

    namespace = parser.parse_args(sys.argv[1:])

    try:
        device_service_name = namespace.name if namespace.name else None

    except Exception as ex:
        _message = _MESSAGES_LIST["e000001"].format(str(sys.argv), ex)

        _logger.error(_message)
        raise exceptions.InvalidServiceNameError(_message)

    return device_service_name


try:
    plugin = _handling_input_parameters()

except exceptions.InvalidServiceNameError:
    plugin = None

if plugin is None:
    plugin = 'CoAP'

if plugin is None:
    print("Required argument 'name' is missing" , file=sys.stderr)
    sys.exit(1)

else:
    Server.start(plugin)
