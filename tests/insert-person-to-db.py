"""
Asyncio for PersonClass 
"""
import asyncio

from unittest import TestCase
from unittest.mock import MagicMock


# One method on this class is an asyncio coroutine.
class PersonClass(object):
    def __init__(self, name=None, cursor=None):
        """Constructor for PersonClass."""
        self.name = name
        self.cursor = cursor
        self.insert_sql = 'INSERT INTO person VALUES ?;'


    async def create(self):  # ....HOW TO TEST THIS METHOD??
        """Persist the person to the database."""
        return await self.cursor.execute(self.insert_sql, (self.name,))


class TestPersonClass(TestCase):
    def test_send_query_on_create(self):
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
            person = PersonClass(name='Johnny', cursor=database_cursor)

            # Call person.create() to trigger the database call
            person_create_response = await person.create()

            # Assert the response from person.create() is our predefined future
            assert person_create_response == 'future result!'
            # Assert the database cursor was called once
            execute_stub.assert_called_once_with(person.insert_sql, ('Johnny',))

        # Run the async test
        coro = asyncio.coroutine(run_test)
        event_loop.run_until_complete(coro())
        event_loop.close()


