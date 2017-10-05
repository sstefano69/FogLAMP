#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" Device service exceptions module contains Exception subclasses
"""

__author__ = "Stefano Simonelli"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


class InvalidServiceNameError(Exception):
    """ Invalid device service name"""
    pass


class InvalidPluginTypeError(Exception):
    """ Invalid plugin type, only the type -device- is allowed """
    pass


class DataRetrievalError(Exception):
    """ Unable to retrieve data from the device """
    pass
