"""
Originally from: https://gist.github.com/Terris-ScaleDB/25755471da431056d54b45ff8f4b9296 
"""
import datetime
import logging
import asyncio

import aiocoap.resource as resource
import aiocoap

from cbor2 import loads, dumps

import sqlalchemy as sa
from aiopg.sa import create_engine
from sqlalchemy.dialects.postgresql import JSON, JSONB

metadata = sa.MetaData()

tbl = sa.Table(
    'asset_readings'
    , metadata
    , sa.Column('data', JSONB))

class CoapIngest(resource.Resource):
    def __init__(self):
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

# logging setup
logging.basicConfig(level=logging.INFO)
logging.getLogger("coap-server").setLevel(logging.DEBUG)

def main():
    # Resource tree creation
    root = resource.Site()

    root.add_resource(('.well-known', 'core'), resource.WKCResource(root.get_resources_as_linkheader))

    root.add_resource(('other','ingest'), CoapIngest())

    asyncio.Task(aiocoap.Context.create_server_context(root))
    asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    main()

 
