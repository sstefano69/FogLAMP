"""The following tests the configuration manager component"""
from foglamp.configuration_manager import (create_category, _configuration_tbl)
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


@pytest.fixture(scope="module")
async def _new_category()->dict:
    """
    INSERT information found in data dictionary into configuration table
    :return:
        data: A dictionary with hardcoded values in configuration table
            data = {key:{description:str, value:json}}
                json={info:{description:str, type:str(data_type},
                      default:str(data_type_value)}}
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
    for name in data:
        await create_category(category_name=name,
                              category_description=data[name]['category_description'],
                              category_value=data[name]['category_value'])
    return data


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("configuration_manager")
async def test_new_category():
    """
    Test the _delete_from_configuration() and _new_category() methods that are used in
    consequent tests behave as expected
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

    data = await _new_category()
    try:
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for count in conn.execute(stmt):
                    assert count[0] == len(list(data.keys()))
    except Exception:
        print("QUERY Failed")
        raise
    for key in data.keys():
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
            print("QUERY Failed")
            raise
    await _delete_from_configuration()


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("configuration_manager")
async def test_new_category_original_items_true():
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
    data = await _new_category()
    await create_category(category_name='boolean',
                          category_description=data['boolean']['category_description'],
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
                        data['boolean']['category_value']['info']['description'])
                    assert result[0]['info']['type'] == (
                        data['boolean']['category_value']['info']['type'])
                    assert result[0]['info']['default'] == (
                        data['boolean']['category_value']['info']['default'])
    except Exception:
        print('Query failed')
        raise
    await _delete_from_configuration


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("configuration_manager")
async def test_new_category_original_items_false():
    """
    Test the behavior of create_category when keep_original_items == False
    :assert:
        1. `values` dictionary only has 'data' category
        2. values in 'data' category are as expected
    """
    stmt = sa.select([_configuration_tbl.c.value]).select_from(_configuration_tbl).where(
        _configuration_tbl.c.key == 'boolean')
    await _delete_from_configuration()
    data = await _new_category()
    await create_category(category_name='boolean',
                          category_description=data['boolean']['category_description'],
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
        print('Query failed')
        raise
    await _delete_from_configuration


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("configuration_manager")
async def test_create_category_error():
    """
    Test that ValueError is returned when category_value.type is invalid
    :assert:
        1. Assert error type
        2. Assert list of valid  category_value.types is returned
        3. There aren't any rows in configuration table
    """
    await _delete_from_configuration()
    with pytest.raises(ValueError) as error_exec:
        await create_category(category_name='float', category_description='float type',
                              category_value={
                                  'info': {
                                      'description': 'float type with default 1.1',
                                      'type': 'float',
                                      'default': '1.1'}})
    assert 'ValueError' in str(error_exec)
    assert ("['boolean', 'integer', 'string', 'IPv4', 'IPv6', 'X509 certificate', "
            + "'password', 'JSON']") in str(error_exec)

    stmt = sa.select([sa.func.count()]).select_from(_configuration_tbl)
    try:
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for count in conn.execute(stmt):
                    assert count[0] == 0
    except Exception:
        print("QUERY Failed")
        raise
    await _delete_from_configuration()

