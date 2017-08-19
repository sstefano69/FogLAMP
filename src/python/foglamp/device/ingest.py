# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""FogLAMP Sensor Readings Ingest API"""

import asyncio
import datetime
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

_LOGGER = logger.setup(__name__)  # logging.Logger

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

    _insert_queue = None  # type: asyncio.Queue
    """insert objects are added to this queue"""

    _insert_tasks = None  # type: List[asyncio.Task]
    """asyncio task for :meth:`_insert_readings`"""

    _get_next_insert_tasks = None  # type: List[asyncio.Task]
    """asyncio task for asyncio.Queue.get called by :meth:`_insert_readings`"""

    # Configuration
    _max_inserts_per_transaction = 5
    """Number of inserts to perform into the readings table in a single transaction"""
    _max_transactions = 5
    """Number of open database transactions"""
    _max_queued_inserts = 100
    """Maximum pending inserts at any given time"""

    @classmethod
    async def start(cls):
        """Starts the server"""
        if cls._started:
            return

        # Read config
        """read config
        _max_inserts_per_transaction
        _max_transactions
        _max_queued_inserts
        """

        cls._insert_queue = asyncio.Queue(maxsize=cls._max_queued_inserts)

        # Start asyncio tasks
        cls._write_statistics_task = asyncio.ensure_future(cls._write_statistics())

        cls._insert_tasks = []
        cls._get_next_insert_tasks = []

        for _ in range(0, cls._max_transactions-1):
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

        cls._get_next_insert_tasks = None

        for _ in cls._insert_tasks:
            await _

        cls._insert_tasks = None
        cls._insert_queue = None

        # Write statistics
        if cls._write_statistics_sleep_task is not None:
            cls._write_statistics_sleep_task.cancel()
            cls._write_statistics_sleep_task = None

        await cls._write_statistics_task
        cls._write_statistics_task = None

        cls._started = False
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

            try:
                async with aiopg.sa.create_engine(_CONNECTION_STRING, minsize=1,
                                                  maxsize=cls._max_transactions) as engine:
                    async with engine.acquire() as conn:
                        # Get more inserts to perform, up to a total according to config

                        inserts = [insert]
                        insert = None

                        for _ in range(2, cls._max_inserts_per_transaction):
                            try:
                                inserts.append(cls._insert_queue.get_nowait())
                            except asyncio.QueueEmpty:
                                break

                        for _ in inserts:
                            try:
                                await conn.execute(_)
                                cls._readings += 1
                            except psycopg2.IntegrityError as e:
                                # This exception is also thrown for NULL violations
                                _LOGGER.info('Duplicate key inserting sensor values.\n%s\n%s',
                                             _, e)
                            except Exception:
                                cls._discarded_readings += 1
                                _LOGGER.exception('Insert failed: %s', _)
                            finally:
                                cls._insert_queue.task_done()
            except Exception:
                cls._discarded_readings += 1
                _LOGGER.exception('Database connection failed: %s', _CONNECTION_STRING)
            finally:
                if insert is not None:
                    cls._insert_queue.task_done()

        _LOGGER.info('Insert readings loop stopped')

    @classmethod
    async def _write_statistics(cls):
        """Periodically commits collected readings statistics"""
        _LOGGER.info("Device statistics writer started")

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
                _LOGGER.exception("An error occurred while writing readings statistics")

        _LOGGER.info("Device statistics writer stopped")

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
            raise RuntimeError("Service is stopping")
        # Assume the code beyond this point doesn't 'await'
        # to make sure that the queue is not appended to
        # when cls._stop is True

        if not cls._started:
            raise RuntimeError('The ingest server has not started')

        try:
            if asset is None:
                raise ValueError("asset can not be None")

            if not isinstance(asset, str):
                asset = str(asset)

            if timestamp is None:
                raise ValueError("timestamp can not be None")

            if not isinstance(timestamp, datetime.datetime):
                # validate
                timestamp = dateutil.parser.parse(timestamp)

            if key is not None and not isinstance(key, uuid.UUID):
                # Validate
                key = uuid.UUID(key)

            if readings is None:
                readings = dict()
            elif not isinstance(readings, dict):
                # Postgres allows values like 5 be converted to JSON
                # Downstream processors can not handle this
                raise TypeError("readings must be a dict")
        except Exception:
            cls.increment_discarded_readings()
            raise

        # Comment out to test IntegrityError
        # key = '123e4567-e89b-12d3-a456-426655440000'

        insert = _READINGS_TBL.insert()
        insert = insert.values(asset_code=asset,
                               reading=readings,
                               read_key=key,
                               user_ts=timestamp)

        _LOGGER.debug('Database command: %s', insert)
        await cls._insert_queue.put(insert)

