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

import asyncpg
import dateutil.parser
import json

from foglamp import logger
from foglamp import statistics


__author__ = "Terris Linenbach"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

# _LOGGER = logger.setup(__name__)  # type: logging.Logger
_LOGGER = logger.setup(__name__, level=logging.DEBUG)  # type: logging.Logger
# _LOGGER = logger.setup(__name__, destination=logger.CONSOLE, level=logging.DEBUG)

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

    _queues = None  # type: List[asyncio.Queue]
    """insert objects are added to these queues"""

    _current_queue_index = 0
    """Which queue to insert into next"""

    _insert_readings_tasks = None  # type: List[asyncio.Task]
    """asyncio tasks for :meth:`_insert_readings`"""

    _insert_readings_wait_tasks = None  # type: List[asyncio.Task]
    """asyncio tasks for asyncio.Queue.get called by :meth:`_insert_readings`"""

    # 3/50/5 - 200
    # 3/50/1 - 500
    # 3/100/1 - 283
    # 3/10/1 - 217
    # 3/50/1 - 312
    # 3/50/1 - 463
    # 5/100/1 - 466
    # 5/50/2 - 466
    # 5/500/2 - 539
    # 10/500/5 - 539
    # 10/50/1 - 515
    # 3/500/5 - 434
    # 3/10/5 - 481

    # Configuration
    _num_queues = 3
    """Maximum number of insert queues. Each queue has its own database connection."""

    _batch_size = 10
    """Maximum number of rows in a batch of inserts"""

    _queue_flush_seconds = 5
    """Number of seconds to wait for a queue to reach the maximum batch size"""

    _max_queue_size = 3*_batch_size
    """Maximum number of items in a queue"""

    _max_insert_attempts = 100
    """Number of times to attempt to insert a batch"""

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

        for _ in range(cls._num_queues):
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

        cls._stop = False

    @classmethod
    def increment_discarded_readings(cls):
        """Increments the number of discarded sensor readings"""
        cls._discarded_readings += 1

    @classmethod
    async def _insert_readings(cls, queue_index):
        """Inserts rows into the readings table using _queue

        Uses "copy" to load rows into a temp table and then
        inserts the temp table into the readings table because
        "copy" does not support "on conflict ignore"
        """
        _LOGGER.info('Insert readings loop started')

        queue = cls._queues[queue_index]  # type: asyncio.Queue
        connection = None  # type: asyncpg.connection.Connection
        loop_ctr = 10*cls._queue_flush_seconds

        while True:
            # Wait for enough items in the queue to fill a batch
            for _ in range(loop_ctr):
                if cls._stop:
                    break
                if queue.qsize() >= cls._batch_size:
                    break
                #_LOGGER.debug('Waiting: Queue index: %s Queue size: %s',
                #              queue_index, queue.qsize())
                await asyncio.sleep(.1)

            # Wait for the first entry
            try:
                insert = queue.get_nowait()
            except asyncio.QueueEmpty:
                if cls._stop:
                    break

                # Wait for an item in the queue
                waiter = asyncio.ensure_future(queue.get())
                cls._insert_readings_wait_tasks[queue_index] = waiter

                try:
                    insert = await waiter
                except asyncio.CancelledError:
                    continue
                finally:
                    cls._insert_readings_wait_tasks[queue_index] = None

            #inserts = [(insert[0], insert[1], insert[2], json.dumps(insert[3]))]
            inserts = [insert]

            for _ in range(1, cls._batch_size):
                try:
                    insert = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                inserts.append(insert)

            while True:
                try:
                    if connection is None:
                        connection = await asyncpg.connect(database='foglamp')
                        # Create a temp table for 'copy' command
                        await connection.execute('create temp table t_readings ('
                                                 'asset_code character varying(50),'
                                                 'user_ts timestamp(6) with time zone,'
                                                 'read_key uuid,'
                                                 'reading jsonb)')
                    else:
                        await connection.execute('truncate table t_readings')

                    await connection.copy_records_to_table(
                        table_name='t_readings',
                        records=inserts)

                    await connection.execute('insert into readings(asset_code,user_ts,read_key,'
                                             'reading) select * from t_readings '
                                             'on conflict do nothing')

                    cls._readings += len(inserts)
                    #_LOGGER.debug('Queue index: %s Batch size: %s', queue_index, len(inserts))

                    break
                except Exception as e:
                    next_attempt = _ + 1
                    _LOGGER.exception('Insert failed on attempt #%s', next_attempt)

                    if cls._stop or next_attempt == cls._max_insert_attempts:
                        cls._discarded_readings += len(inserts)
                        break
                    else:
                        if connection is None:
                            await asyncio.sleep(1)
                        else:
                            try:
                                await connection.close()
                            except Exception:
                                _LOGGER.exception('Closing connection failed')
                            connection = None

        # Exiting this method
        if connection is not None:
            try:
                await connection.close()
            except Exception:
                _LOGGER.exception('Closing connection failed')

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
        """Indicates whether all queues are currently full

        Returns:
            False - All of the queues are empty
            True - Otherwise
        """
        if cls._stop:
            return False

        queue_index = cls._current_queue_index
        if cls._queues[queue_index].qsize() < cls._max_queue_size:
            return True

        for _ in range(1, cls._num_queues):
            queue_index += 1
            if queue_index >= cls._num_queues:
                queue_index = 0
            if cls._queues[queue_index].qsize() < cls._max_queue_size:
                cls._current_queue_index = queue_index
                return True

        _LOGGER.info('Unavailable')
        return False

    @classmethod
    async def add_readings(cls, asset: str, timestamp: Union[str, datetime.datetime],
                           key: Union[str, uuid.UUID] = None, readings: dict = None)->None:
        """Adds an asset readings record to FogLAMP

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

        cls.is_available()
        queue_index = cls._current_queue_index
        queue = cls._queues[queue_index]
        await queue.put((asset, timestamp, key, json.dumps(readings)))

        #_LOGGER.debug('Queue index: %s Queue size: %s', cls._current_queue_index, queue.qsize())

