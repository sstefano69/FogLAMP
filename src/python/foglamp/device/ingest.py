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
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg

from foglamp import logger
from foglamp import statistics


__author__ = "Terris Linenbach"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__)  # type: logging.Logger
# _LOGGER = logger.setup(__name__, destination=logger.CONSOLE, level=logging.DEBUG)
_DEBUG = _LOGGER.isEnabledFor(logging.DEBUG)

_READINGS_TBL = sa.Table(
    'readings',
    sa.MetaData(),
    sa.Column('asset_code', sa.types.VARCHAR(50)),
    sa.Column('read_key', sa.types.VARCHAR(50)),
    sa.Column('user_ts', sa.types.TIMESTAMP),
    sa.Column('reading', pg.JSONB))
"""Defines the table that data will be inserted into"""

_CONNECTION_STRING = "dbname='foglamp'"

_STATISTICS_WRITE_FREQUENCY_SECONDS = 5


class Ingest(object):
    """Adds sensor readings to FogLAMP

    Also tracks readings-related statistics.

    Readings are added to a configurable number of queues. These queues are processed
    concurrently. Each queue is assigned to a database connection. Queued items are
    batched into a single insert transaction. The size of these batches have a
    configurable maximum and minimum.
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

    _queues = None  # type: List[asyncio.Queue]
    """insert objects are added to these queues"""

    _current_queue_index = 0
    """Which queue to insert into next"""

    _insert_readings_tasks = None  # type: List[asyncio.Task]
    """asyncio tasks for :meth:`_insert_readings`"""

    _insert_readings_wait_tasks = None  # type: List[asyncio.Task]
    """asyncio tasks for asyncio.Queue.get called by :meth:`_insert_readings`"""

    # Configuration
    _queue_count = 1
    """Maximum number of open database connections"""

    _max_inserts_per_transaction = 100
    """Maximum number of inserts per transaction"""

    _queue_flush_seconds = 1  # _queue_count*.1
    """Number of seconds to wait for a queue to reach _max_inserts_per_transaction"""

    _max_queue_size = (_queue_count+2)*_max_inserts_per_transaction
    """Maximum number of pending inserts before returning 'busy'"""

    @classmethod
    async def start(cls):
        """Starts the server"""
        if cls._started:
            return

        # TODO: Read config

        # Start asyncio tasks
        cls._write_statistics_task = asyncio.ensure_future(cls._write_statistics())

        cls._insert_readings_tasks = []
        cls._insert_readings_wait_tasks = []
        cls._queues = []

        for _ in range(cls._queue_count):
            cls._queues.append(asyncio.Queue(maxsize=cls._max_queue_size))
            cls._insert_readings_wait_tasks.append(None)
            cls._insert_readings_tasks.append(asyncio.ensure_future(cls._insert_readings(_)))

        cls._started = True

    @classmethod
    async def stop(cls):
        """Stops the server

        Flushes pending statistics and readings to the database
        """
        if cls._stop or not cls._started:
            return

        cls._stop = True

        for _ in cls._insert_readings_wait_tasks:
            if _ is not None:
                _.cancel()

        for _ in cls._queues:
            await _.join()

        _LOGGER.info("All queues flushed")

        for _ in cls._insert_readings_tasks:
            await _

        cls._started = False

        cls._insert_readings_wait_tasks = None
        cls._insert_readings_tasks = None
        cls._queues = None

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
    async def _insert_readings(cls, queue_num):
        """Inserts rows into the readings table using _queue"""
        _LOGGER.info('Insert readings loop started')

        queue = cls._queues[queue_num]

        while True:
            # Wait for the maximum number of rows
            for _ in range(10*cls._queue_flush_seconds):
                if cls._stop:
                    break
                if queue.qsize() >= cls._max_inserts_per_transaction:
                    break
                # _LOGGER.debug('Waiting: Queue index: %s Queue size: %s',
                #               queue_num, queue.qsize())
                #waiter = asyncio.ensure_future(asyncio.sleep(cls._queue_flush_seconds))
                #cls._insert_readings_wait_tasks[queue_num] = waiter
                #try:
                    #await waiter
                #except asyncio.CancelledError:
                    #pass
                #finally:
                    #cls._insert_readings_wait_tasks[queue_num] = None
                await asyncio.sleep(.1)

            insert = None

            try:
                insert = queue.get_nowait()
                queue.task_done()
            except asyncio.QueueEmpty:
                if cls._stop:
                    break

                # Wait for an item in the queue
                waiter = asyncio.ensure_future(queue.get())
                cls._insert_readings_wait_tasks[queue_num] = waiter

                try:
                    insert = await waiter
                    queue.task_done()
                except asyncio.CancelledError:
                    break
                finally:
                    cls._insert_readings_wait_tasks[queue_num] = None

            num_inserts = 1

            try:
                if cls._engine is None:
                    cls._engine = await aiopg.sa.create_engine(_CONNECTION_STRING,
                                                               minsize=cls._queue_count)

                # Consume all entries in the queue
                # Create a transaction for every X inserts
                async with cls._engine.acquire() as conn:
                    while True:
                        if insert is None:
                            try:
                                insert = queue.get_nowait()
                                queue.task_done()
                                num_inserts += 1
                            except asyncio.QueueEmpty:
                                break

                        async with conn.begin() as tx:
                            for _ in range(cls._max_inserts_per_transaction):
                                if insert is None:
                                    try:
                                        insert = queue.get_nowait()
                                        queue.task_done()
                                        num_inserts += 1
                                    except asyncio.QueueEmpty:
                                        break

                                insert_stmt = pg.insert(_READINGS_TBL).values(asset_code=insert[0],
                                                                              user_ts=insert[1],
                                                                              read_key=insert[2],
                                                                              reading=insert[3])

                                # _LOGGER.debug('Database command: %s', insert_stmt)

                                # insert_stmt can not be converted to string after this line
                                insert_stmt = insert_stmt.on_conflict_do_nothing(
                                                            index_elements=['read_key'])

                                await conn.execute(insert_stmt)
                                insert = None

                        # Transaction is committed
                        cls._readings += num_inserts
                        # _LOGGER.debug('Queue index: %s Num inserts: %s', queue_num, num_inserts)
                        num_inserts = 0

                        if queue.qsize() < cls._max_inserts_per_transaction:
                            # Give the connection back to the pool
                            break
            except Exception:
                # Rollback
                cls._discarded_readings += num_inserts
                # insert_stmt can not be converted to string
                _LOGGER.exception('Insert failed: %s', insert)

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
    def is_available(cls) -> bool:
        if cls._stop:
            return False

        queue_num = cls._current_queue_index
        if cls._queues[queue_num].qsize() < cls._max_inserts_per_transaction:
            return True

        for _ in range(cls._queue_count):
            queue_num += 1
            if queue_num >= cls._queue_count:
                queue_num = 0
            if cls._queues[queue_num].qsize() < cls._max_inserts_per_transaction:
                cls._current_queue_index = queue_num
                return True

        for _ in range(cls._queue_count):
            if cls._queues[queue_num].qsize() < cls._max_queue_size:
                cls._current_queue_index = queue_num
                return True
            queue_num += 1
            if queue_num >= cls._queue_count:
                queue_num = 0

        # _LOGGER.debug("Not available")
        return False

    @classmethod
    async def add_readings(cls, asset: str, timestamp: Union[str, datetime.datetime],
                           key: Union[str, uuid.UUID] = None, readings: dict = None)->None:
        """Adds an asset readings record to FogLAMP

        Proper usage of this method:
            This method should be called only if and immediately after
            :meth:`is_available` returns True. There should be no asyncio context
            switches between method calls. Failure to follow these requirements
            can cause an asyncio.QueueFull exception to be raised.

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

            RuntimeError:
                The server is stopping or has been stopped

            ValueError, TypeError:
                An invalid value was provided

            asyncio.QueueFull:
                All of the queues are full. The caller should wait and retry.
        """
        if cls._stop:
            raise RuntimeError('The device server is stopping')
        # Assume the code beyond this point doesn't 'await'
        # to make sure that the queue is not appended to
        # when cls._stop is True

        if not cls._started:
            raise RuntimeError('The device server was not started')
            # cls._logger = logger.setup(__name__, destination=logger.CONSOLE, level=logging.DEBUG)

        try:
            if asset is None:
                raise ValueError('asset can not be None')

            if not isinstance(asset, str):
                raise TypeError('asset must be a string')

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

        queue = cls._queues[cls._current_queue_index]
        queue.put_nowait((asset, timestamp, key, readings))

        # _LOGGER.debug('Queue index: %s Queue size: %s', queue_num, queue.qsize())

