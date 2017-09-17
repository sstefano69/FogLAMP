"""The following tests the configuration manager component"""
from foglamp.configuration_manager import (create_category, _configuration_tbl,
                                           register_interest, set_category_item_value_entry)
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
async def test_new_category_default_data_types():
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
async def test_new_category_keep_original_items_true():
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
async def test_new_category_keep_original_items_false():
    """
    Test the behavior of create_category when keep_original_items == False
    :assert:
        1. `values` dictionary only has 'data' category
        2. values in 'data' category are as expected
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
        1. Assert that TypeError gets returned when  type is invalid (used 'float)
        2. Asert that ValueError gets returned when entry_name type is missing from item_name
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

    await _delete_from_configuration()


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("configuration_manager")
async def test_register_interest_errors():
    """
    Test that appropriate errors are returned for register_interest
    :assert:
        1. An error is returned when category_name = None
        2. An error is retund when callback = None
    """
    await _delete_from_configuration()
    with pytest.raises(ValueError) as error_exec:
        register_interest(category_name=None, callback='foglamp.callback')
    assert 'ValueError: Failed to register interest. category_name cannot be None' in (
        str(error_exec))

    await _delete_from_configuration()
    with pytest.raises(ValueError) as error_exec:
        register_interest(category_name='boolean', callback=None)
    assert 'ValueError: Failed to register interest. callback cannot be None' in (
        str(error_exec))
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
