"""
Using async/io print 'Hello World'
URL: http://asyncio.readthedocs.io/en/latest/hello_world.html
"""
import asyncio
from unittest.mock import MagicMock
from unittest import TestCase

class HelloWorld(object):
    def __init__(self,when=1,what="Hello World"):
        self.when=when
        self.what=what

    async def say(self):
        await asyncio.sleep(self.when)
        return(self.what)

class TestHelloWorld(TestCase):
    def test_send_query_on_create(self):
        """Should send insert query to database on create()"""
        event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(event_loop)

        async def run_test():
            person = HelloWorld(when=1,what="Hello World")

            person_create_response = await person.say()
            assert person_create_response == "Goodbye World"

        coro = asyncio.coroutine(run_test)
        event_loop.run_until_complete(coro())
        event_loop.close()