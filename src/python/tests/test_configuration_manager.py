"""
The following tests the configuration manager component For the most part,
the code uses the boolean type for testing due to simplicity; but contains
tests to verify which data_types are supported and which are not.
"""
from foglamp.configuration_manager import (create_category, _configuration_tbl,
                                           set_category_item_value_entry, get_category_item,
                                           get_category_item_value_entry, get_category_all_items,
                                           get_all_category_names)
import aiopg
import pytest
import sqlalchemy as sa

__author__ = "Ori Shadmon"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_CONNECTION_STRING = "dbname='foglamp'"


@pytest.fixture(scope="module")
async def _delete_from_configuration():
    try:
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                await conn.execute("DELETE FROM foglamp.configuration")
    except Exception:
        print("DELETE Failed")
        raise

@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("configuration_manager")
async def test_create_category_default_data_types():
    """
    Test the _delete_from_configuration() method works properly, and that the expected
    (supported) data types do get inserted without an error.
    :assert:
        1. No data exists in configuration
        2. All data was inserted into configuration
        3. Each column in configuration table contains valid row information for
        each category
    """
    await _delete_from_configuration()
    stmt = sa.select([sa.func.count()]).select_from(_configuration_tbl)
    try:
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for count in conn.execute(stmt):
                    assert count[0] == 0
    except Exception:
        print("QUERY Failed")
        raise

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
    await _delete_from_configuration()
    stmt = sa.select([sa.func.count()]).select_from(_configuration_tbl)
    try:
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for count in conn.execute(stmt):
                    assert count[0] == 0
    except Exception:
        print("QUERY Failed: %s" % stmt)
        raise


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("configuration_manager")
async def test_create_category_keep_original_items_true():
    """
    Test the behavior of create_category when keep_original_items == True
    :assert:
        1. `values` dictionary has both categories
        2. values in 'data' category are as expected
        3. values in 'info' category did not change
    """
    stmt = sa.select([_configuration_tbl.c.value]).select_from(_configuration_tbl).where(
        _configuration_tbl.c.key == 'boolean')
    await _delete_from_configuration()

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

    await _delete_from_configuration()


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("configuration_manager")
async def test_create_category_keep_original_items_false():
    """
    Test the behavior of create_category when keep_original_items == False
    :assert:
        1. `values` dictionary only has 'data' category
        2. values in 'data' category are as expected
    """
    stmt = sa.select([_configuration_tbl.c.value]).select_from(_configuration_tbl).where(
        _configuration_tbl.c.key == 'boolean')
    await _delete_from_configuration()

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

    await _delete_from_configuration()


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("configuration_manager")
async def test_create_category_errors():
    """
    Test that create_category returns the expected error for each case
    :assert:
        1. Assert that TypeError gets returned when  type is invalid (used 'float')
        2. Asert that ValueError gets returned when entry_name type is missing from item_name
        3. Assert that TypeError gets returned when entry_name type is None
        4. Assert that TypeError gets returned when entry_name type is an actual value,
            rather than the string of it (specifically Boolean and Integer types)
    """
    await _delete_from_configuration()
    with pytest.raises(ValueError) as error_exec:
        await create_category(category_name='float', category_description='float type',
                              category_value={
                                  'info': {
                                      'description': 'float type with default 1.1',
                                      'type': 'float',
                                      'default': '1.1'}})
    assert ('ValueError: Invalid entry_val for entry_name "type" for item_name info. valid: '+
            "['boolean', 'integer', 'string', 'IPv4', " +
            "'IPv6', 'X509 certificate', 'password', 'JSON']") in str(error_exec)

    await _delete_from_configuration()
    with pytest.raises(ValueError) as error_exec:
        await create_category(category_name='float', category_description='float type',
                              category_value={
                                  'info': {
                                      'description': 'float type with default 1.1',
                                      'default': '1.1'}})
    assert "ValueError: Missing entry_name type for item_name info" in str(error_exec)

    await _delete_from_configuration()
    with pytest.raises(ValueError) as error_exec:
        await create_category(category_name='float', category_description='float type',
                              category_value={
                                  'info': {
                                      'type': 'integer',
                                      'default': '1'}})
    assert "ValueError: Missing entry_name description for item_name info" in str(error_exec)

    await _delete_from_configuration()
    with pytest.raises(ValueError) as error_exec:
        await create_category(category_name='float', category_description='float type',
                              category_value={
                                  'info': {
                                      'type': 'integer',
                                      'default': 'integer type with value 1'}})
    assert "ValueError: Missing entry_name description for item_name info" in str(error_exec)

    with pytest.raises(TypeError) as error_exec:
        await create_category(category_name='boolean',
                              category_description='boolean type',
                              category_value={'data': {
                                  'description': 'boolean type with default True',
                                  'type': 'boolean',
                                  'default': None}},
                              keep_original_items=False)
    assert "TypeError: entry_val must be a string for item_name data and entry_name default" in str(error_exec)

    with pytest.raises(TypeError) as error_exec:
        await create_category(category_name='boolean',
                              category_description='boolean type',
                              category_value={'data': {
                                  'description': 'boolean type with default True',
                                  'type': None,
                                  'default': 'False'}},
                              keep_original_items=False)
    assert "TypeError: entry_val must be a string for item_name data and entry_name type" in str(error_exec)

    with pytest.raises(TypeError) as error_exec:
        await create_category(category_name='boolean',
                          category_description='boolean type',
                          category_value={'data': {
                              'description': 'boolean type with default True',
                              'type': 'boolean',
                              'default': False}},
                          keep_original_items=False)
    assert "TypeError: entry_val must be a string for item_name data and entry_name default" in str(error_exec)

    with pytest.raises(TypeError) as error_exec:
        await create_category(category_name='integer',
                          category_description='integer type',
                          category_value={'data': {
                              'description': 'integers type with default o',
                              'type': 'integer',
                              'default': 0}},
                          keep_original_items=False)
    assert "TypeError: entry_val must be a string for item_name data and entry_name default" in str(error_exec)
    await _delete_from_configuration()


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("configuration_manager")
async def test_set_category_item_value_entry():
    """
    Test updating of configuration.value for a specific key
    :assert:
        1. `default` and `value` in configuration.value are the same
        2. `value` in configuration.value gets updated, while `default` does not
    """
    await _delete_from_configuration()
    stmt = sa.select([_configuration_tbl.c.value]).select_from(_configuration_tbl).where(
        _configuration_tbl.c.key == 'boolean')
    await _delete_from_configuration()
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
    await _delete_from_configuration()

@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.feature("configuration_manager")
async def test_get_category_item_value_entry():
    """
    Test that get_category_item_value_entry works properly
    :Assert:
        1. category_value.value gets returned and matchates default
        2. When updating value, the data retrieved for value gets updated
    """
    await _delete_from_configuration()
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
    await _delete_from_configuration()

@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.feature("configuration_manager")
async def test_get_category_item_value_entry_empty():
    """
    Test that None gets returned when either category_name and/or item_name don't exist
    :assert:
        1. Assert None is returned when item_name is does not exist
        2. Assert None is returned when category_name does not exist
    """
    await _delete_from_configuration()
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
    await _delete_from_configuration()


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.feature("configuration_manager")
async def test_get_category_item():
    """
    Test that gt_category_item returns all the data in configuration.value for a specific category_name
    :assert:
        1. Information in configuration.value match the category_values declared
        2. When updating value, the data retrieved for default is as expected
    """
    await _delete_from_configuration()
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

    await _delete_from_configuration()

@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.feature("configuration_manager")
async def test_get_category_item_empty():
    """
    Test that gt_category_item when either category_name or item_name do not exist
    :assert:
        Assert result is None when category_name or item_name do not exist in configuration
    """
    await _delete_from_configuration()
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
    await _delete_from_configuration()

@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.feature("configuration_manager")
async def test_get_category_all_items():
    """
    Test get_category_all_items method returns full "dictionary" of category_value
    :assert:
        1.  Values in dictionary are as expcted
        2. default doesn't get updated when value does
    """
    await  _delete_from_configuration()
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

    await  set_category_item_value_entry(category_name='boolean', item_name='info', new_value_entry='True')
    result = await get_category_all_items(category_name='boolean')
    assert result['info']['default'] == 'False'
    assert result['info']['value'] == 'True'

    await _delete_from_configuration()

@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.feature("configuration_manager")
async def test_get_category_all_items_empty():
    """
    Test get_category_all_items doesn't return anything if category_name doesn't exist
    :assert:
        Assert None gets returned when category_name does not exist
    """
    await  _delete_from_configuration()
    await create_category(category_name='boolean', category_description='boolean type',
                          category_value={
                              'info': {
                                  'description': 'boolean type with default False',
                                  'type': 'boolean',
                                  'default': 'False'}
                          })

    result = await get_category_all_items(category_name='integer')
    assert result is None
    await _delete_from_configuration()

@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.feature("configuration_manager")
async def test_get_all_category_names():
    """
    Test get_all_category_names validly returns the category_name and category_description
    :assert:
        Assert that the category_name retrieved corresponds to the category_description with the use
        of the existing dictionary (`data`)
    """
    await _delete_from_configuration()
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
        assert data[result[0].replace(" ","")]['category_description'] == result[1]
    await _delete_from_configuration()