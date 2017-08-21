# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""FogLAMP Sensor Readings Ingest API"""

import asyncio
import datetime
import logging
import uuid
from typing import List, Union

import aiopg.sa
import dateutil.parser
import psycopg2
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from foglamp import logger
from foglamp import statistics


__author__ = "Terris Linenbach"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__)  # type: logging.Logger
_DEBUG = _LOGGER.isEnabledFor(logging.DEBUG)

_READINGS_TBL = sa.Table(
    'readings',
    sa.MetaData(),
    sa.Column('asset_code', sa.types.VARCHAR(50)),
    sa.Column('read_key', sa.types.VARCHAR(50)),
    sa.Column('user_ts', sa.types.TIMESTAMP),
    sa.Column('reading', JSONB))
"""Defines the table that data will be inserted into"""

_CONNECTION_STRING = "dbname='foglamp'"

_STATISTICS_WRITE_FREQUENCY_SECONDS = 5


class Ingest(object):
    """Adds sensor readings to FogLAMP

    Also tracks readings-related statistics.
    """

    # Class attributes
    _readings = 0  # type: int
    """Number of readings accepted before statistics were flushed to storage"""

    _discarded_readings = 0  # type: int
    """Number of readings rejected before statistics were flushed to storage"""

    _write_statistics_task = None  # type: asyncio.Task
    """asyncio task for :meth:`_write_statistics`"""

    _write_statistics_sleep_task = None  # type: asyncio.Task
    """asyncio task for asyncio.sleep"""

    _stop = False
    """Set to true when the server needs to stop"""

    _started = False
    """True when the server has been started"""

    _engine = None  # type: aiopg.sa.Engine
    """Database connection pool"""

    _insert_queue = None  # type: asyncio.Queue
    """insert objects are added to this queue"""

    _insert_tasks = None  # type: List[asyncio.Task]
    """asyncio tasks for :meth:`_insert_readings`"""

    _get_next_insert_tasks = None  # type: List[asyncio.Task]
    """asyncio tasks for asyncio.Queue.get called by :meth:`_insert_readings`"""

    # Configuration
    _max_db_connections = 1
    """Maximum number of open database db_connections"""

    _max_inserts_per_transaction = 1
    """Maximum number of inserts per transaction"""

    @classmethod
    async def start(cls):
        """Starts the server"""
        if cls._started:
            return

        # Read config
        """read config
        _max_db_connections
         *** validate > 0 ***
        """
        cls._insert_queue = asyncio.Queue(maxsize=
                                          max(30, (2+cls._max_db_connections)*(
                                              cls._max_inserts_per_transaction)))

        # Start asyncio tasks
        cls._write_statistics_task = asyncio.ensure_future(cls._write_statistics())

        cls._insert_tasks = []
        cls._get_next_insert_tasks = []

        for _ in range(cls._max_db_connections):
            cls._insert_tasks.append(asyncio.ensure_future(cls._insert_readings(_)))
            cls._get_next_insert_tasks.append(None)

        cls._started = True

    @classmethod
    async def stop(cls):
        """Stops the server

        Flushes pending statistics and readings to the database
        """
        if cls._stop or not cls._started:
            return

        cls._stop = True

        await cls._insert_queue.join()

        for _ in cls._get_next_insert_tasks:
            if _ is not None:
                _.cancel()

        for _ in cls._insert_tasks:
            await _

        cls._started = False

        cls._get_next_insert_tasks = None
        cls._insert_tasks = None
        cls._insert_queue = None

        # Write statistics
        if cls._write_statistics_sleep_task is not None:
            cls._write_statistics_sleep_task.cancel()
            cls._write_statistics_sleep_task = None

        await cls._write_statistics_task
        cls._write_statistics_task = None

        if cls._engine is not None:
            cls._engine.close()
            cls._engine = None

        cls._stop = False

    @classmethod
    def increment_discarded_readings(cls):
        """Increments the number of discarded sensor readings"""
        cls._discarded_readings += 1

    @classmethod
    async def _insert_readings(cls, task_num):
        """Inserts rows into the readings table using _insert_queue"""
        _LOGGER.info('Insert readings loop started')

        while True:
            insert = None

            try:
                insert = cls._insert_queue.get_nowait()
            except asyncio.QueueEmpty:
                if cls._stop:
                    break

                # Wait for an item in the queue
                waiter = asyncio.ensure_future(cls._insert_queue.get())
                cls._get_next_insert_tasks[task_num] = waiter

                try:
                    insert = await waiter
                except asyncio.CancelledError:
                    break
                finally:
                    cls._get_next_insert_tasks[task_num] = None

            num_inserts = 1

            try:
                if cls._engine is None:
                    cls._engine = await aiopg.sa.create_engine(_CONNECTION_STRING,
                                                               minsize=cls._max_db_connections)

                # Consume all entries in the queue
                # Create a transaction for every X inserts
                async with cls._engine.acquire() as conn:
                    while True:
                        if insert is None:
                            try:
                                insert = cls._insert_queue.get_nowait()
                                num_inserts += 1
                            except asyncio.QueueEmpty:
                                break

                        async with conn.begin() as tx:
                            for _ in range(cls._max_inserts_per_transaction):
                                if insert is None:
                                    try:
                                        insert = cls._insert_queue.get_nowait()
                                        num_inserts += 1
                                    except asyncio.QueueEmpty:
                                        break

                                insert = _READINGS_TBL.insert().values(asset_code=insert[0],
                                                                       user_ts=insert[1],
                                                                       read_key=insert[2],
                                                                       reading=insert[3])

                                if _DEBUG:
                                    _LOGGER.debug('Database command: %s', insert)

                                try:
                                    await conn.execute(insert)
                                except psycopg2.IntegrityError as e:
                                    # This exception is also thrown for NULL violations
                                    if cls._max_inserts_per_transaction == 1:
                                        num_inserts = 0
                                    else:
                                        raise

                                cls._insert_queue.task_done()
                                insert = None

                        # Commit
                        cls._readings += num_inserts
                        num_inserts = 0
            except Exception:
                # Rollback
                cls._discarded_readings += num_inserts
                cls._insert_queue.task_done()
                _LOGGER.exception('Insert failed')

        _LOGGER.info('Insert readings loop stopped')

    @classmethod
    async def _write_statistics(cls):
        """Periodically commits collected readings statistics"""
        _LOGGER.info('Device statistics writer started')

        while not cls._stop:
            # stop() calls _write_statistics_sleep_task.cancel().
            # Tracking _write_statistics_sleep_task separately is cleaner than canceling
            # this entire coroutine because allowing database activity to be
            # interrupted will result in strange behavior.
            cls._write_statistics_sleep_task = asyncio.ensure_future(
                asyncio.sleep(_STATISTICS_WRITE_FREQUENCY_SECONDS))

            try:
                await cls._write_statistics_sleep_task
            except asyncio.CancelledError:
                pass

            cls._write_statistics_sleep_task = None

            try:
                await statistics.update_statistics_value('READINGS', cls._readings)
                cls._readings = 0

                await statistics.update_statistics_value('DISCARDED', cls._discarded_readings)
                cls._discarded_readings = 0
            # TODO catch real exception
            except Exception:
                _LOGGER.exception('An error occurred while writing readings statistics')

        _LOGGER.info('Device statistics writer stopped')

    @classmethod
    async def add_readings(cls, asset: str, timestamp: Union[str, datetime.datetime],
                           key: Union[str, uuid.UUID] = None, readings: dict = None)->None:
        """Add asset readings to FogLAMP

        Args:
            asset: Identifies the asset to which the readings belong
            timestamp: When the readings were taken
            key:
                Unique key for these readings. If this method is called multiple with the same
                key, the readings are only written to the database once
            readings: A dictionary of sensor readings

        Raises:
            If this method raises an Exception, the discarded readings counter is
            also incremented.

            ValueError, TypeError:
                An invalid value was provided
        """
        if cls._stop:
            raise RuntimeError('The device server is stopping')
        # Assume the code beyond this point doesn't 'await'
        # to make sure that the queue is not appended to
        # when cls._stop is True

        if not cls._started:
            raise RuntimeError('The device server was not started')

        try:
            if asset is None:
                raise ValueError('asset can not be None')

            if not isinstance(asset, str):
                asset = str(asset)

            if timestamp is None:
                raise ValueError('timestamp can not be None')

            if not isinstance(timestamp, datetime.datetime):
                # validate
                timestamp = dateutil.parser.parse(timestamp)

            if key is not None and not isinstance(key, uuid.UUID):
                # Validate
                if not isinstance(key, str):
                    raise TypeError('key must be a uuid.UUID or a string')
                # If key is not a string, uuid.UUID throws an Exception that appears to
                # be a TypeError but can not be caught as a TypeError
                key = uuid.UUID(key)

            if readings is None:
                readings = dict()
            elif not isinstance(readings, dict):
                # Postgres allows values like 5 be converted to JSON
                # Downstream processors can not handle this
                raise TypeError('readings must be a dictionary')
        except Exception:
            cls.increment_discarded_readings()
            raise

        # Comment out to test IntegrityError
        # key = '123e4567-e89b-12d3-a456-426655440000'

        if _DEBUG:
            _LOGGER.debug('Queue size: %s', cls._insert_queue.qsize())

        await cls._insert_queue.put((asset, timestamp, key, readings))

