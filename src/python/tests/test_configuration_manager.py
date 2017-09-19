"""
The following tests the configuration manager component For the most part,
the code uses the boolean type for testing due to simplicity; but contains
tests to verify which data_types are supported and which are not.
"""
from foglamp.configuration_manager import *
from foglamp.configuration_manager import _registered_interests, _configuration_tbl
import aiopg
import pytest
import sqlalchemy as sa

__author__ = "Ori Shadmon"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_CONNECTION_STRING = "dbname='foglamp'"
pytestmark = pytest.mark.asyncio


@pytest.allure.feature("unit")
@pytest.allure.story("configuration_manager")
class TestConfigurationManager:
    """
    The following breaks down each configuration_manager method, and tests
    its errors, and behaviors
    """
    @pytest.fixture(scope="module")
    async def _delete_from_configuration(self):
        """clear data from foglamp.configuration and clear _registered_interests object"""
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    await conn.execute("DELETE FROM foglamp.configuration")
        except Exception:
            print("DELETE Failed")
            raise
        _registered_interests.clear()

    async def test_delete_from_configuration(self):
        """
        Test the _delete_from_configuration() method works properly
        :assert:
            1. No data exists in configuration
            2. _registered_interests is empty
        """
        stmt = sa.select([sa.func.count()]).select_from(_configuration_tbl)
        await self._delete_from_configuration()
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for count in conn.execute(stmt):
                        assert count[0] == 0
        except Exception:
            print("QUERY Failed")
            raise
        assert _registered_interests == {}
        await self._delete_from_configuration()

    async def test_create_configuration_all_data_types(self):
        """
        Test that the accepted data types succeed
        :Assert:
            1. All data was inserted into configuration
            2. Each column in configuration table contains valid row information for
            each category
        """
        stmt = sa.select([sa.func.count()]).select_from(_configuration_tbl)
        await self._delete_from_configuration()
        data = {
            'boolean': {'category_description': 'boolean type',
                        'category_value': {
                            'info': {
                                'description': 'boolean type with default False',
                                'type': 'boolean',
                                'default': 'False'}}},
            'integer': {'category_description': 'integer type',
                        'category_value': {
                            'info': {
                                'description': 'integer type with default 1',
                                'type': 'integer',
                                'default': '1'}}},
            'string': {'category_description': 'string type',
                       'category_value': {
                           'info': {
                               'description': "string type with default 'ABCabc'",
                               'type': 'string',
                               'default': 'ABCabc'}}},
            'JSON': {'category_description': 'JSON type',
                     'category_value': {
                         'info': {
                             'description': "JSON type with default {}",
                             'type': 'JSON',
                             'default': '{}'}}},
            'IPv4': {'category_description': 'IPv4 type',
                     'category_value': {
                         'info': {
                             'description': "IPv4 type with default '127.0.0.1'",
                             'type': 'IPv4',
                             'default': '127.0.0.1'}}},
            'IPv6': {'category_description': 'IPv6 type',
                     'category_value': {
                         'info': {
                             'description': "IPv6 type with default '2001:db8::'",
                             'type': 'IPv6',
                             'default': '2001:db8::'}}},
            'X509': {'category_description': 'X509 Certification',
                     'category_value': {
                         'info': {
                             'description': "X509 Certification",
                             'type': 'X509 certificate',
                             'default': 'x509_certificate.cer'}}},
            'password': {'category_description': 'Password Type',
                         'category_value': {
                             'info': {
                                 'description': "Password Type with default ''",
                                 'type': 'password',
                                 'default': ''}}}
        }
        for category_name in data:
            await create_category(category_name=category_name,
                                  category_description=data[category_name]['category_description'],
                                  category_value=data[category_name]['category_value'])
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for count in conn.execute(stmt):
                        assert count[0] == len(list(data.keys()))
        except Exception:
            print("QUERY Failed: %s" % stmt)
            raise
        for key in data:
            stmt = sa.select([_configuration_tbl.c.key, _configuration_tbl.c.description,
                              _configuration_tbl.c.value]).select_from(
                                  _configuration_tbl).where(
                                      _configuration_tbl.c.key == key)
            try:
                async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                    async with engine.acquire() as conn:
                        async for result in conn.execute(stmt):
                            assert result[0].replace(" ", "") == key
                            assert result[1] == data[key]['category_description']
                            # category_value dictionary assert
                            assert list(result[2].keys())[0] == "info"
                            assert result[2]['info']['description'] == data[key][
                                'category_value']['info']['description']
                            assert result[2]['info']['type'] == data[key]['category_value'][
                                'info']['type']
                            assert result[2]['info']['default'] == data[key]['category_value'][
                                'info']['default']
            except Exception:
                print("QUERY Failed: %s" % stmt)
                raise
        await self._delete_from_configuration()

    async def test_create_category_keep_original_items_true(self):
        """
        Test the behavior of create_category when keep_original_items == True
        :assert:
            1. `values` dictionary has both categories
            2. values in 'data' category are as expected
            3. values in 'info' category did not change
        """
        stmt = sa.select([_configuration_tbl.c.value]).select_from(_configuration_tbl).where(
            _configuration_tbl.c.key == 'boolean')
        await self._delete_from_configuration()

        await create_category(category_name='boolean', category_description='boolean type',
                              category_value={
                                  'info': {
                                      'description': 'boolean type with default False',
                                      'type': 'boolean',
                                      'default': 'False'}})

        await create_category(category_name='boolean',
                              category_description='boolean type',
                              category_value={'data': {
                                  'description': 'boolean type with default True',
                                  'type': 'boolean',
                                  'default': 'True'}},
                              keep_original_items=True)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert sorted(list(result[0].keys())) == ['data', 'info']
                        assert result[0]['data']['description'] == (
                            'boolean type with default True')
                        assert result[0]['data']['type'] == 'boolean'
                        assert result[0]['data']['default'] == 'True'
                        assert result[0]['info']['description'] == (
                            'boolean type with default False')
                        assert result[0]['info']['type'] == 'boolean'
                        assert result[0]['info']['default'] == 'False'
        except Exception:
            print('Query failed: %s' % stmt)
            raise

        await self._delete_from_configuration()

    async def test_create_category_keep_original_items_false(self):
        """
        Test the behavior of create_category when keep_original_items == False
        :assert:
            1. initial `info` data has been added
            2. `values` dictionary only has 'data' category
            3. values in 'data' category are as expected
        """
        stmt = sa.select([_configuration_tbl.c.value]).select_from(_configuration_tbl).where(
            _configuration_tbl.c.key == 'boolean')
        await self._delete_from_configuration()
        await create_category(category_name='boolean', category_description='boolean type',
                              category_value={'info': {
                                  'description': 'boolean type with default False',
                                  'type': 'boolean',
                                  'default': 'False'}})
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert sorted(list(result[0].keys())) == ['info']
                        assert result[0]['info']['description'] == (
                            'boolean type with default False')
                        assert result[0]['info']['type'] == 'boolean'
                        assert result[0]['info']['default'] == 'False'
        except Exception:
            print('Query failed: %s' % stmt)
            raise

        await create_category(category_name='boolean',
                              category_description='boolean type',
                              category_value={'data': {
                                  'description': 'boolean type with default True',
                                  'type': 'boolean',
                                  'default': 'True'}},
                              keep_original_items=False)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert sorted(list(result[0].keys())) == ['data']
                        assert result[0]['data']['description'] == (
                            'boolean type with default True')
                        assert result[0]['data']['type'] == 'boolean'
                        assert result[0]['data']['default'] == 'True'
        except Exception:
            print('Query failed: %s' % stmt)
            raise

        await self._delete_from_configuration()

    async def test_create_category_invalid_type(self):
        """
        Test that create_category returns the expected error when type is invalid
        :assert:
            Assert that TypeError gets returned when  type is invalid (used 'float')
        """
        await self._delete_from_configuration()
        with pytest.raises(ValueError) as error_exec:
            await create_category(category_name='float', category_description='float type',
                                  category_value={
                                      'info': {
                                          'description': 'float type with default 1.1',
                                          'type': 'float',
                                          'default': '1.1'}})
        assert ('ValueError: Invalid entry_val for entry_name "type" for item_name info. valid: ' +
                "['boolean', 'integer', 'string', 'IPv4', " +
                "'IPv6', 'X509 certificate', 'password', 'JSON']") in str(error_exec)
        await self._delete_from_configuration()

        await self._delete_from_configuration()
        with pytest.raises(ValueError) as error_exec:
            await create_category(category_name='INTEGER', category_description='INTEGER type',
                                  category_value={
                                      'info': {
                                          'description': 'INTEGER type with default 1',
                                          'type': 'INTEGER',
                                          'default': '1'}})
        assert ('ValueError: Invalid entry_val for entry_name "type" for item_name info. valid: ' +
                "['boolean', 'integer', 'string', 'IPv4', " +
                "'IPv6', 'X509 certificate', 'password', 'JSON']") in str(error_exec)
        await self._delete_from_configuration()

    async def test_create_category_invalid_entry_value(self):
        """
        Test cases where value is set to the actual "value" rather than the string of the value
        :Assert:
            1. Assert TypeError when type is set to bool rather than 'boolean'
            2. Assert TypeError when default is set to False rather than 'False'
        """
        await self._delete_from_configuration()
        with pytest.raises(TypeError) as error_exec:
            await create_category(category_name='boolean', category_description='boolean type',
                                  category_value={'info': {
                                      'description': 'boolean type with default False',
                                      'type': bool,
                                      'default': 'False'
                                  }})
        assert ("TypeError: entry_val must be a string for item_name " +
                "info and entry_name type") in str(error_exec)
        await self._delete_from_configuration()
        with pytest.raises(TypeError) as error_exec:
            await create_category(category_name='boolean',
                                  category_description='boolean type',
                                  category_value={'info': {
                                      'description': 'boolean type with default False',
                                      'type': 'boolean',
                                      'default': False
                                  }})
        assert ("TypeError: entry_val must be a string for item_name " +
                "info and entry_name default") in str(error_exec)
        await self._delete_from_configuration()

    async def test_create_category_missing_entry(self):
        """
        Test that create_category returns the expected error when one of the entry names
        is missing
        :assert:
            1. Assert ValueError when type is missing
            2. Assert ValueError when description is missing
            3. Assert ValueError when default is missing
        """
        await self._delete_from_configuration()
        with pytest.raises(ValueError) as error_exec:
            await create_category(category_name='boolean', category_description='boolean type',
                                  category_value={
                                      'info': {
                                          'description': 'boolean type with default False',
                                          'default': 'False'}})
        assert "ValueError: Missing entry_name type for item_name info" in str(error_exec)

        await self._delete_from_configuration()
        with pytest.raises(ValueError) as error_exec:
            await create_category(category_name='boolean', category_description='boolean type',
                                  category_value={
                                      'info': {
                                          'type': 'boolean',
                                          'default': 'False'}})
        assert "ValueError: Missing entry_name description for item_name info" in str(error_exec)

        await self._delete_from_configuration()
        with pytest.raises(ValueError) as error_exec:
            await create_category(category_name='boolean', category_description='boolean type',
                                  category_value={
                                      'info': {
                                          'type': 'integer',
                                          'description': 'integer type with value False'}})
        assert "ValueError: Missing entry_name default for item_name info" in str(error_exec)
        await self._delete_from_configuration()

    async def test_create_category_invalid_entry_none(self):
        """
        Test that TypeError is returned when entry_name is None
        :Assert:
            1. Assert TypeError when description is None
            2. Assert TypeError when type is None
            3. Assert TypeError when default is None
        """
        await self._delete_from_configuration()
        with pytest.raises(TypeError) as error_exec:
            await create_category(category_name='boolean', category_description='boolean type',
                                  category_value={'info': {
                                      'description': None,
                                      'type': 'boolean',
                                      'default': 'False'
                                  }})
        assert ("TypeError: entry_val must be a string for item_name " +
                "info and entry_name description") in str(error_exec)

        await self._delete_from_configuration()
        with pytest.raises(TypeError) as error_exec:
            await create_category(category_name='boolean', category_description='boolean type',
                                  category_value={'info': {
                                      'description': 'boolean type with default False ',
                                      'type': None,
                                      'default': 'False'
                                  }})
        assert ("TypeError: entry_val must be a string for item_name " +
                "info and entry_name type") in str(error_exec)

        await self._delete_from_configuration()
        with pytest.raises(TypeError) as error_exec:
            await create_category(category_name='boolean', category_description='boolean type',
                                  category_value={'info': {
                                      'description': 'boolean type with default False ',
                                      'type': 'boolean',
                                      'default': None
                                  }})
        assert ("TypeError: entry_val must be a string for item_name info " +
                "and entry_name default") in str(error_exec)
        await self._delete_from_configuration()

    async def test_set_category_item_value_entry(self):
        """
        Test updating of configuration.value for a specific key
        :assert:
            1. `default` and `value` in configuration.value are the same
            2. `value` in configuration.value gets updated, while `default` does not
        """
        await self._delete_from_configuration()
        stmt = sa.select([_configuration_tbl.c.value]).select_from(_configuration_tbl).where(
            _configuration_tbl.c.key == 'boolean')
        await self._delete_from_configuration()
        await create_category(category_name='boolean', category_description='boolean type',
                              category_value={
                                  'info': {
                                      'description': 'boolean type with default False',
                                      'type': 'boolean',
                                      'default': 'False'}})
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0]['info']['value'] == 'False'
                        assert result[0]['info']['default'] == 'False'
        except Exception:
            print('Query failed: %s' % stmt)
            raise

        await set_category_item_value_entry(category_name='boolean',
                                            item_name='info', new_value_entry='True')
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0]['info']['value'] == 'True'
                        assert result[0]['info']['default'] == 'False'
        except Exception:
            print('Query failed: %s' % stmt)
            raise
        await self._delete_from_configuration()

    async def test_get_category_item_value_entry(self):
        """
        Test that get_category_item_value_entry works properly
        :Assert:
            1. category_value.value gets returned and matches default
            2. When updating value, the data retrieved for value gets updated
        """
        await self._delete_from_configuration()
        await create_category(category_name='boolean', category_description='boolean type',
                              category_value={
                                  'info': {
                                      'description': 'boolean type with default False',
                                      'type': 'boolean',
                                      'default': 'False'}
                              })
        result = await get_category_item_value_entry(category_name='boolean', item_name='info')
        assert result == 'False'

        await set_category_item_value_entry(category_name='boolean',
                                            item_name='info', new_value_entry='True')
        result = await get_category_item_value_entry(category_name='boolean', item_name='info')
        assert result == 'True'
        await self._delete_from_configuration()

    async def test_get_category_item_value_entry_empty(self):
        """
        Test that None gets returned when either category_name and/or item_name don't exist
        :assert:
            1. Assert None is returned when item_name does not exist
            2. Assert None is returned when category_name does not exist
        """
        await self._delete_from_configuration()
        await create_category(category_name='boolean', category_description='boolean type',
                              category_value={
                                  'info': {
                                      'description': 'boolean type with default False',
                                      'type': 'boolean',
                                      'default': 'False'}
                              })
        result = await get_category_item_value_entry(category_name='boolean', item_name='data')
        assert result is None

        result = await get_category_item_value_entry(category_name='integer', item_name='info')
        assert result is None
        await self._delete_from_configuration()

    async def test_get_category_item(self):
        """
        Test that get_category_item returns all the data in configuration.
        value for a specific category_name
        :assert:
            1. Information in configuration.value match the category_values declared
            2. When updating value, the data retrieved for default is as expected
        """
        await self._delete_from_configuration()
        await create_category(category_name='boolean', category_description='boolean type',
                              category_value={
                                  'info': {
                                      'description': 'boolean type with default False',
                                      'type': 'boolean',
                                      'default': 'False'}
                              })
        result = await get_category_item(category_name='boolean', item_name='info')
        assert result['description'] == 'boolean type with default False'
        assert result['type'] == 'boolean'
        assert result['default'] == 'False'
        assert result['value'] == 'False'

        await set_category_item_value_entry(category_name='boolean',
                                            item_name='info', new_value_entry='True')
        result = await get_category_item(category_name='boolean', item_name='info')
        assert result['default'] == 'False'
        assert result['value'] == 'True'

        await self._delete_from_configuration()

    async def test_get_category_item_empty(self):
        """
        Test that gt_category_item when either category_name or item_name do not exist
        :assert:
            Assert result is None when category_name or item_name do not exist in configuration
        """
        await self._delete_from_configuration()
        await create_category(category_name='boolean', category_description='boolean type',
                              category_value={
                                  'info': {
                                      'description': 'boolean type with default False',
                                      'type': 'boolean',
                                      'default': 'False'}
                              })
        result = await get_category_item(category_name='integer', item_name='info')
        assert result is None

        result = await get_category_item(category_name='boolean', item_name='data')
        assert result is None
        await self._delete_from_configuration()

    async def test_get_category_all_items(self):
        """
        Test get_category_all_items method returns full "dictionary" of category_value
        :assert:
            1.  Values in dictionary are as expected
            2. default doesn't get updated when value does
        """
        await  self._delete_from_configuration()
        await create_category(category_name='boolean', category_description='boolean type',
                              category_value={
                                  'info': {
                                      'description': 'boolean type with default False',
                                      'type': 'boolean',
                                      'default': 'False'}
                              })

        result = await get_category_all_items(category_name='boolean')
        assert result['info']['description'] == 'boolean type with default False'
        assert result['info']['type'] == 'boolean'
        assert result['info']['default'] == 'False'
        assert result['info']['value'] == 'False'

        await  set_category_item_value_entry(category_name='boolean', item_name='info',
                                             new_value_entry='True')
        result = await get_category_all_items(category_name='boolean')
        assert result['info']['default'] == 'False'
        assert result['info']['value'] == 'True'

        await self._delete_from_configuration()

    async def test_get_category_all_items_empty(self):
        """
        Test get_category_all_items doesn't return anything if category_name doesn't exist
        :assert:
            Assert None gets returned when category_name does not exist
        """
        await self._delete_from_configuration()
        await create_category(category_name='boolean', category_description='boolean type',
                              category_value={
                                  'info': {
                                      'description': 'boolean type with default False',
                                      'type': 'boolean',
                                      'default': 'False'}
                              })

        result = await get_category_all_items(category_name='integer')
        assert result is None
        await self._delete_from_configuration()

    async def test_get_all_category_names(self):
        """
        Test get_all_category_names validly returns the category_name and category_description
        :assert:
            Assert that the category_name retrieved corresponds to the category_description with
            the use of the existing dictionary (`data`)
        """
        await self._delete_from_configuration()
        data = {
            'boolean': {'category_description': 'boolean type',
                        'category_value': {
                            'info': {
                                'description': 'boolean type with default False',
                                'type': 'boolean',
                                'default': 'False'}}},
            'integer': {'category_description': 'integer type',
                        'category_value': {
                            'info': {
                                'description': 'integer type with default 1',
                                'type': 'integer',
                                'default': '1'}}},
            'string': {'category_description': 'string type',
                       'category_value': {
                           'info': {
                               'description': "string type with default 'ABCabc'",
                               'type': 'string',
                               'default': 'ABCabc'}}},
            'JSON': {'category_description': 'JSON type',
                     'category_value': {
                         'info': {
                             'description': "JSON type with default {}",
                             'type': 'JSON',
                             'default': '{}'}}},
            'IPv4': {'category_description': 'IPv4 type',
                     'category_value': {
                         'info': {
                             'description': "IPv4 type with default '127.0.0.1'",
                             'type': 'IPv4',
                             'default': '127.0.0.1'}}},
            'IPv6': {'category_description': 'IPv6 type',
                     'category_value': {
                         'info': {
                             'description': "IPv6 type with default '2001:db8::'",
                             'type': 'IPv6',
                             'default': '2001:db8::'}}},
            'X509': {'category_description': 'X509 Certification',
                     'category_value': {
                         'info': {
                             'description': "X509 Certification",
                             'type': 'X509 certificate',
                             'default': 'x509_certificate.cer'}}},
            'password': {'category_description': 'Password Type',
                         'category_value': {
                             'info': {
                                 'description': "Password Type with default ''",
                                 'type': 'password',
                                 'default': ''}}}
        }
        for category_name in data:
            await create_category(category_name=category_name,
                                  category_description=data[category_name]['category_description'],
                                  category_value=data[category_name]['category_value'])
        results = await get_all_category_names()
        for result in results:
            assert data[result[0].replace(" ", "")]['category_description'] == result[1]
        await self._delete_from_configuration()

    async def test_register_interest(self):
        """
        Test that when register_interest is called, _registered_interests gets updated
        :assert:
            1. when index of keys list is 0, corresponding name is 'boolean'
            2. the value for _register_interests['boolean'] is {'tests.callback'}
        """
        await self._delete_from_configuration()
        register_interest(category_name='boolean', callback='tests.callback')
        assert list(_registered_interests.keys())[0] == 'boolean'
        assert _registered_interests['boolean'] == {'tests.callback'}
        await self._delete_from_configuration()

    async def test_register_interest_error(self):
        """
        Test that error gets returned when either category_name or callback is None
        :Assert:
            Assert error message when category_name is None
            Assert error message when callback is None
            Assert error messages when both are None
        """
        await self._delete_from_configuration()
        with pytest.raises(ValueError) as error_exec:
            register_interest(category_name=None, callback='foglamp.callback')
        assert "ValueError: Failed to register interest. category_name cannot be None" in (
            str(error_exec))

        with pytest.raises(ValueError) as error_exec:
            register_interest(category_name='integer', callback=None)
        assert "ValueError: Failed to register interest. callback cannot be None" in (
            str(error_exec))
        await self._delete_from_configuration()
