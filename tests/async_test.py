"""
Originally from: https://gist.github.com/Terris-ScaleDB/25755471da431056d54b45ff8f4b9296 
"""
import datetime
import logging
import asyncio
# import async
import aiocoap.resource as resource
import aiocoap

from cbor2 import loads, dumps

import sqlalchemy as sa
from aiopg.sa import create_engine
from sqlalchemy.dialects.postgresql import JSON, JSONB

from unittest import TestCase
from unittest import MagicMock

metadata = sa.MetaData()

tbl = sa.Table(
    'asset_readings'
    , metadata
    , sa.Column('data', JSONB))


class CoapIngest(resource.Resource):
    def __init__(self, ):
        super(CoapIngest, self).__init__()

    async def render_post(self, request):
        r = loads(request.payload)
        async with create_engine(user='postgres'
                , database='foglamp'
                , host='192.168.0.220'
                , password='foglamp') as engine:
            async with engine.acquire() as conn:
                await conn.execute(tbl.insert().values(data=r))
        return aiocoap.Message(payload=''.encode("utf-8"))


class TestCoapIngest(TestCase):
    def test_send_request_on_render(self):
        """Should send insert query to database on create()"""
        event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(event_loop)

        async def run_test():

            # Stub the database cursor
            database_cursor = MagicMock()

            # Stub the execute function with mocked results from the database
            execute_stub = MagicMock(return_value='future result!')
            # Wrap the stub in a coroutine (so it can be awaited)
            execute_coro = asyncio.coroutine(execute_stub)
            database_cursor.execute = execute_coro

            # Instantiate new person obj
            coap_ingest = CoapIngest()

            # Call person.create() to trigger the database call
            coap_ingest_create_response = await coap_ingest.render_post()

            # Assert the response from person.create() is our predefined future
            assert coap_ingest_create_response == 'future result!'
            # Assert the database cursor was called once
            execute_stub.assert_called_once_with(coap_ingest)

        # Run the async test
        coro = asyncio.coroutine(run_test)
        event_loop.run_until_complete(coro())
        event_loop.close()
