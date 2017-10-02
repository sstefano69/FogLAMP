"""
The following tests the configuration manager component For the most part,
the code uses the boolean type for testing due to simplicity; but contains
tests to verify which data_types are supported and which are not.
"""

import asyncio
import os
import pytest
import sqlalchemy as sa
import aiopg.sa
from foglamp.configuration_manager import (create_category, set_category_item_value_entry,
                                           register_interest, get_all_category_names,
                                           get_category_all_items, get_category_item,
                                           get_category_item_value_entry, _registered_interests,
                                           _configuration_tbl)
__author__ = "Ori Shadmon"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_CONNECTION_STRING = "dbname='foglamp'"

pytestmark = pytest.mark.asyncio

async def delete_from_configuration():
    """Remove initial data from configuration table"""
    async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
         async with engine.acquire() as conn:
             await conn.execute(_configuration_tbl.delete())
 

@pytest.allure.feature("unit")
@pytest.allure.story("configuration manager")
class TestConfigurationManager:
    """
    The following breaks down each configuration_manager method, and tests
    its errors, and behaviors
    """

    def setup_method(self):
        """reset foglamp data in database, with the exception of
        configuration (which should be empty), and clear data (if
        exists) in _registered_interests object"""

        asyncio.get_event_loop().run_until_complete(delete_from_configuration())
        _registered_interests.clear()

    def teardown_method(self):
        """reset foglamp data in database, and clear data (if exists)
        in _registered_interests object"""
        asyncio.get_event_loop().run_until_complete(delete_from_configuration())
        _registered_interests.clear()

    async def test_accepted_data_types(self):
        """
        Test that the accepted data types get inserted by using:
            - create_category
            - get_all_category_names (category_name and category_description)
            - get_category_all_items (category_value by category_name)
        :Assert:
            1. Assert that the number of values returned by get_all_category_names
                equals len(data)
            2. category_description returned with get_all_category_names correlates to the
                correct ke
            3. get_category_all_items returns valid category_values for a given key
        """
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
                                  category_value=data[category_name]['category_value'],
                                  keep_original_items=True)

        select_count_stmt = sa.select([sa.func.count()]).select_from(_configuration_tbl)
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for count in conn.execute(select_count_stmt):
                    assert int(count[0]) == len(data)

        categories = await get_all_category_names()
        assert len(categories) == len(data)
        for category in categories:
            key = category[0].strip()
            assert data[key]['category_description'] == category[1]
            category_info = await get_category_all_items(category_name=key)
            assert data[key]['category_value']['info']['description'] == (
                category_info['info']['description'])
            assert data[key]['category_value']['info']['type'] == (
                category_info['info']['type'])
            assert data[key]['category_value']['info']['default'] == (
                category_info['info']['default'])

    async def test_create_category_keep_original_items_true(self):
        """
        Test the behavior of create_category when keep_original_items == True
        :assert:
            1. `values` dictionary has both categories
            2. values in 'data' category are as expected
            3. values in 'info' category did not change
        """
        await create_category(category_name='boolean', category_description='boolean type',
                              category_value={
                                  'info': {
                                      'description': 'boolean type with default False',
                                      'type': 'boolean',
                                      'default': 'False'}},
                              keep_original_items=False)

        await create_category(category_name='boolean',
                              category_description='boolean type',
                              category_value={'data': {
                                  'description': 'int type with default 0',
                                  'type': 'integer',
                                  'default': '0'}},
                              keep_original_items=True)

        category_info = await get_category_all_items(category_name='boolean')
        # Both category_values exist
        assert sorted(list(category_info.keys())) == ['data', 'info']
        # Verify 'info' category_value
        assert category_info['info']['description'] == 'boolean type with default False'
        assert category_info['info']['type'] == 'boolean'
        assert category_info['info']['default'] == 'False'
        # Verify 'data' category_value
        assert category_info['data']['description'] == 'int type with default 0'
        assert category_info['data']['type'] == 'integer'
        assert category_info['data']['default'] == '0'

    async def test_create_category_keep_original_items_false(self):
        """
        Test the behavior of create_category when keep_original_items == False
        :assert:
            1. initial `info` data has been added
            2. `values` dictionary only has 'data' category
            3. values in 'data' category are as expected
        """
        await create_category(category_name='boolean', category_description='boolean type',
                              category_value={'info': {
                                  'description': 'boolean type with default False',
                                  'type': 'boolean',
                                  'default': 'False'}})

        await create_category(category_name='boolean',
                              category_description='boolean type',
                              category_value={'data': {
                                  'description': 'int type with default 0',
                                  'type': 'integer',
                                  'default': '0'}},
                              keep_original_items=False)

        category_info = await get_category_all_items(category_name='boolean')
        # only 'data category_values exist
        assert sorted(list(category_info.keys())) == ['data']
        # Verify 'data' category_value
        assert category_info['data']['description'] == 'int type with default 0'
        assert category_info['data']['type'] == 'integer'
        assert category_info['data']['default'] == '0'

    async def test_set_category_item_value_entry(self):
        """
        Test updating of configuration.value for a specific key using
            - create_category to create the category
            - get_category_item_value_entry to check category_value
            - set_category_item_value_entry to update category_value
        :assert:
            1. `default` and `value` in configuration.value are the same
            2. `value` in configuration.value gets updated, while `default` does not
        """
        select_value_stmt = sa.select([_configuration_tbl.c.value]).select_from(
            _configuration_tbl).where(_configuration_tbl.c.key == 'boolean')
        await create_category(category_name='boolean', category_description='boolean type',
                              category_value={
                                  'info': {
                                      'description': 'boolean type with default False',
                                      'type': 'boolean',
                                      'default': 'False'}})
        result = await get_category_item_value_entry(category_name='boolean', item_name='info')
        assert result == 'False'

        await set_category_item_value_entry(category_name='boolean',
                                            item_name='info', new_value_entry='True')
        result = await get_category_item_value_entry(category_name='boolean', item_name='info')
        assert result == 'True'

    async def test_get_category_item(self):
        """
        Test that get_category_item returns all the data in configuration.
        value for a specific category_name
        :assert:
            Information in configuration.value match the category_values declared

        """
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
    
    async def test_create_category_invalid_dict(self):
        """
        Test that create_category returns the expected error when category_value
          is a 'string' rather than a JSON
        :assert:
            Assert that TypeError gets returned when type is string
        """
        with pytest.raises(TypeError) as error_exec:
            await create_category(category_name='integer', category_description='integer type',
                                  category_value='1')
        assert "TypeError: category_val must be a dictionary" in str(error_exec)

    async def test_create_category_invalid_type(self):
        """
        Test that create_category returns the expected error when type is invalid
        :assert:
            Assert that TypeError gets returned when type is float
        """
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

    async def test_create_category_case_sensitive_type(self):
        """
        Test that create_category returns the expected error when type is upper case
        :assert:
            Assert that TypeError gets returned when type is INTEGER
        """
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

    async def test_create_category_invalid_entry_value_for_type(self):
        """
        Test case where value is set to the actual "value" rather than the string of the value
        :Assert:
            Assert TypeError when type is set to bool rather than 'boolean'
        """
        with pytest.raises(TypeError) as error_exec:
            await create_category(category_name='boolean', category_description='boolean type',
                                  category_value={'info': {
                                      'description': 'boolean type with default False',
                                      'type': bool,
                                      'default': 'False'
                                  }})
        assert ("TypeError: entry_val must be a string for item_name " +
                "info and entry_name type") in str(error_exec)

    async def test_create_category_invalid_entry_value_for_default(self):
        """
        Test case where value is set to the actual "value" rather than the string of the value
        :Assert:
            Assert TypeError when default is set to False rather than 'False'
        """
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

    async def test_create_category_invalid_entry_none_for_description(self):
        """
        Test case where value is set to the actual "value" rather than the string of the value
        :Assert:
            Assert TypeError when description is set to None rather than ''
        """
        with pytest.raises(TypeError) as error_exec:
            await create_category(category_name='boolean',
                                  category_description='boolean type',
                                  category_value={'info': {
                                      'description': None,
                                      'type': 'boolean',
                                      'default': 'False'
                                  }})
        assert ("TypeError: entry_val must be a string for item_name " +
                "info and entry_name description") in str(error_exec)

    async def test_create_category_missing_entry_for_type(self):
        """
        Test that create_category returns the expected error when
        category_value entry_name is missing
        :assert:
            Assert ValueError when type is missing
        """
        with pytest.raises(ValueError) as error_exec:
            await create_category(category_name='boolean', category_description='boolean type',
                                  category_value={
                                      'info': {
                                          'description': 'boolean type with default False',
                                          'default': 'False'}})
        assert "ValueError: Missing entry_name type for item_name info" in str(error_exec)

    async def test_create_category_missing_entry_for_description(self):
        """
        Test that create_category returns the expected error when
        category_value entry_name is missing
        :assert:
            Assert ValueError when description is missing
        """
        with pytest.raises(ValueError) as error_exec:
            await create_category(category_name='boolean', category_description='boolean type',
                                  category_value={
                                      'info': {
                                          'type': 'boolean',
                                          'default': 'False'}})
        assert "ValueError: Missing entry_name description for item_name info" in str(error_exec)

    async def test_create_category_invalid_entry_none_for_default(self):
        """
        Test that create_category returns the expected error when
        category_value entry_name is missing
        :assert:
            Assert ValueError when default is missing
        """
        with pytest.raises(ValueError) as error_exec:
            await create_category(category_name='boolean', category_description='boolean type',
                                  category_value={
                                      'info': {
                                          'type': 'integer',
                                          'description': 'integer type with value False'}})
        assert "ValueError: Missing entry_name default for item_name info" in str(error_exec)

    async def test_create_category_invalid_entry_none_for_description(self):
        """
        Test case where value is set to the actual "value" rather than the string of the value
        :Assert:
            Assert TypeError when description is set to None rather than ''
        """
        with pytest.raises(TypeError) as error_exec:
            await create_category(category_name='boolean',
                                  category_description='boolean type',
                                  category_value={'info': {
                                      'description': None,
                                      'type': 'boolean',
                                      'default': 'False'
                                  }})
        assert ("TypeError: entry_val must be a string for item_name " +
                "info and entry_name description") in str(error_exec)

    async def test_create_category_invalid_entry_none_for_type(self):
        """
        Test that TypeError is returned when entry_name is None
        :Assert:
            Assert TypeError when type is None
        """
        with pytest.raises(TypeError) as error_exec:
            await create_category(category_name='boolean', category_description='boolean type',
                                  category_value={'info': {
                                      'description': 'boolean type with default False ',
                                      'type': None,
                                      'default': 'False'
                                  }})
        assert ("TypeError: entry_val must be a string for item_name " +
                "info and entry_name type") in str(error_exec)

    async def test_create_category_invalid_entry_none_for_default(self):
        """
        Test that TypeError is returned when entry_name is None
        :Assert:
            Assert TypeError when default is None
        """
        with pytest.raises(TypeError) as error_exec:
            await create_category(category_name='boolean', category_description='boolean type',
                                  category_value={'info': {
                                      'description': 'boolean type with default False ',
                                      'type': 'boolean',
                                      'default': None
                                  }})
        assert ("TypeError: entry_val must be a string for item_name info " +
                "and entry_name default") in str(error_exec)

    async def test_get_all_category_names_error(self):
        await get_all_category_names()

    @pytest.mark.xfail(reason="FOGL-552")
    async def test_set_category_item_value_error(self):
        """
        Test updating of configuration.value when category_name does not exist
        :assert:
            Nothing happens / returned
        :expect:
            FOGL-522: When updating category item value, when the category_name
            doesn't exist, an error should get returned
        """
        select_value_stmt = sa.select([_configuration_tbl.c.value]).select_from(
            _configuration_tbl).where(_configuration_tbl.c.key == 'boolean')
        await set_category_item_value_entry(category_name='boolean',
                                            item_name='info', new_value_entry='True')

        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for result in conn.execute(select_value_stmt):
                    assert result is None

    @pytest.mark.xfail(reason="FOGL-577")
    async def test_get_category_item_value_entry_dne(self):
        """
        Test that None gets returned when either category_name and/or item_name don't exist
        :assert:
            1. Assert None is returned when item_name does not exist
            2. Assert None is returned when category_name does not exist
        """
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

    @pytest.mark.xfail(reason="FOGL-577")
    async def test_get_category_item_empty(self):
        """
        Test that gt_category_item when either category_name or item_name do not exist
        :assert:
            Assert result is None when category_name or item_name do not exist in configuration
        """
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

    @pytest.mark.xfail(reason="FOGL-577")
    async def test_get_category_all_items_dne(self):
        """
        Test get_category_all_items doesn't return anything if category_name doesn't exist
        :assert:
            Assert None gets returned when category_name does not exist
        """
        await create_category(category_name='boolean', category_description='boolean type',
                              category_value={
                                  'info': {
                                      'description': 'boolean type with default False',
                                      'type': 'boolean',
                                      'default': 'False'}
                              })

        result = await get_category_all_items(category_name='integer')
        assert result is None

    async def test_register_interest(self):
        """
        Test that when register_interest is called, _registered_interests gets updated
        :assert:
            1. when index of keys list is 0, corresponding name is 'boolean'
            2. the value for _register_interests['boolean'] is {'tests.callback'}
        """
        register_interest(category_name='boolean', callback='tests.callback')
        assert list(_registered_interests.keys())[0] == 'boolean'
        assert _registered_interests['boolean'] == {'tests.callback'}

    async def test_register_interest_category_name_none_error(self):
        """
        Test that error gets returned when category_name is None
        :Assert:
            Assert error message when category_name is None
        """
        with pytest.raises(ValueError) as error_exec:
            register_interest(category_name=None, callback='foglamp.callback')
        assert "ValueError: Failed to register interest. category_name cannot be None" in (
            str(error_exec))

    async def test_register_interest_callback_none_error(self):
        """
           Test that error gets returned when callback is None
           :Assert:
               Assert error message when callback is None
        """
        with pytest.raises(ValueError) as error_exec:
            register_interest(category_name='integer', callback=None)
        assert "ValueError: Failed to register interest. callback cannot be None" in (
            str(error_exec))

