import pytest
from foglamp.coap.uri_handlers.sensor_values import SensorValues
from foglamp.coap.uri_handlers.sensor_values import SensorValues
from foglamp.coap.uri_handlers.sensor_values import __tbl__
from foglamp.coap.uri_handlers.sensor_values import *
import sqlalchemy
from unittest import TestCase
from cbor2 import dumps
import asyncio
import mock
from mock import MagicMock


def AsyncMock(*args, **kwargs):
    """
    Initiate MagicMock
    Args:
        *args: 
        **kwargs: 

    Returns:

    """
    m = mock.MagicMock(*args, **kwargs)

    async def mock_coro(*args, **kwargs):
        """
        mock_coro function invokes this inner async mock, 
        with all the arguments that were passed by the caller.

        # https://blog.miguelgrinberg.com/post/unit-testing-asyncio-code
        Args:
            *args: 
            **kwargs: 

        Returns:

        """
        return m(*args, **kwargs)

    mock_coro.mock = m
    return mock_coro


class MagicMockConnection(MagicMock):
    """
    Mock connection using MagicMock
    """
    execute = AsyncMock()


class AcquireContextManager(MagicMock):
    """
    asynchronous context manager is a context manager that is able to: 
        suspend execution in its enter (__aenter__) and exit (__aexit__) methods
    https://www.python.org/dev/peps/pep-0492/#id59
    """

    async def __aenter__(self):
        connection = MagicMockConnection()
        return connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MagicMockEngine(MagicMock):
    """
    Mocking an engine connect
    """
    acquire = AcquireContextManager()


class CreateEngineContextManager(MagicMock):
    """ 
    Connect and disconnect from engine connect 
    """

    async def __aenter__(self):
        engine = MagicMockEngine()
        return engine

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class FakeConfig(MagicMock):
    """
    Fake configuration file/info
    """
    db_conn_str = "fake_connection"


def _run(coro):
    """
    using asyncio run coro (request)
    Args:
        coro: 

    Returns:

    """
    return asyncio.get_event_loop().run_until_complete(coro)


# http://docs.sqlalchemy.org/en/latest/core/dml.html
class TestSensorValues(TestCase):
    @mock.patch('aiopg.sa.create_engine')
    @mock.patch('foglamp.configurator.Configurator.__init__')
    def test_render_post(self, test_patch1, test_patch2):
        test_patch1.return_value = None
        test_patch2.return_value = CreateEngineContextManager()  # initiate CreateEngineContextManager
        sv = SensorValues()  # call SensorValues() script
        request = MagicMock()  # Initiate MagicMock
        dict_payload = {'jack': 4098, 'sape': 4139}  # sample data set
        request.payload = dumps(dict_payload)  # dump of sample data set
        returnval = _run(sv.render_post(request))  # execute request
        assert returnval is not None  # verify request result is not empty




