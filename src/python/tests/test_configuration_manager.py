from foglamp.configuration_manager import (create_category, _configuration_tbl, get_all_category_names,
                                           get_category_all_items, get_category_item_value_entry,
                                           get_category_item_value_entry)
import aiopg
import pytest
import sqlalchemy as sa

__author__ = "Ori Shadmon"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_connection_string = "dbname='foglamp'"


async def _delete_from_configuration():
    try:
        async with aiopg.sa.create_engine(_connection_string) as engine:
            async with engine.acquire() as conn:
                await conn.execute("DELETE FROM foglamp.configuration")
    except Exception:
        print("DELETE Failed")
        raise

async def _new_category()->dict:
    """
    INSERT information found in data dictionary into configuration table
    :return:
        data: A dictionary with hardcoded values in configuration table
            data = {key:{description:str, value:json}}
                json={info:{description:str, type:str(data_type}, default:str(data_type_value)}}
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
    for name in data.keys():
        await create_category(category_name=name, category_description=data[name]['category_description'],
                              category_value=data[name]['category_value'])
    return data

@pytest.mark.asyncio
async def test_new_category():
    """
    Test the _delete_from_configuration() and _new_category() methods that are used in consequent tests
    :assert:
        1. No data exists in configuration
        2. All data was inserted into configuration
        3. Each column in configuration table contains valid row information for each category
    """
    await _delete_from_configuration()
    stmt = sa.select([sa.func.count()]).select_from(_configuration_tbl)
    try:
        async with aiopg.sa.create_engine(_connection_string) as engine:
            async with engine.acquire() as conn:
                async for count in conn.execute(stmt):
                    assert count[0] == 0
    except Exception:
        print("QUERY Failed")
        raise

    data = await _new_category()
    try:
        async with aiopg.sa.create_engine(_connection_string) as engine:
            async with engine.acquire() as conn:
                async for count in conn.execute(stmt):
                    assert count[0] == len(list(data.keys()))
    except Exception:
        print("QUERY Failed")
        raise
    for key in data.keys():
        stmt = sa.select([_configuration_tbl.c.key, _configuration_tbl.c.description,
                          _configuration_tbl.c.value]).select_from(_configuration_tbl).where(
            _configuration_tbl.c.key == key)
        try:
            async with aiopg.sa.create_engine(_connection_string) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0].replace(" ","") == key
                        assert result[1] == data[key]['category_description']
                        # category_value dictionary assert
                        assert list(result[2].keys())[0] == "info"
                        assert result[2]['info']['description'] == data[key]['category_value']['info']['description']
                        assert result[2]['info']['type'] == data[key]['category_value']['info']['type']
                        assert result[2]['info']['default'] == data[key]['category_value']['info']['default']
        except Exception:
            print("QUERY Failed")
            raise
    await _delete_from_configuration()

@pytest.mark.asyncio
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
                                    'default': '1.1'}
                            }
                            )
    assert 'ValueError' in str(error_exec)
    assert "['boolean', 'integer', 'string', 'IPv4', 'IPv6', 'X509 certificate', 'password', 'JSON']" in str(error_exec)

    stmt = sa.select([sa.func.count()]).select_from(_configuration_tbl)
    try:
        async with aiopg.sa.create_engine(_connection_string) as engine:
            async with engine.acquire() as conn:
                async for count in conn.execute(stmt):
                    assert count[0] == 0
    except Exception:
        print("QUERY Failed")
        raise


    await _delete_from_configuration()

@pytest.mark.asyncio
async def test_get_all_category_names():
    """
    Test that the names and descriptions of all categories are returned
    :assert:
        1. Nothing gets returned since there isn't data in the table
        2. The number of sets returned is equal to the number of keys in data
        3. The description in data (based on key/category_description) is the same as the description in result
    """
    await _delete_from_configuration()
    results = await get_all_category_names()
    assert results == []
    data = await _new_category()
    results = await get_all_category_names()
    assert len(results) == len(data.keys())
    for result in results:
        assert data[result[0].replace(" ", "")]['category_description'] == result[1]
    await _delete_from_configuration()

@pytest.mark.asyncio
async def test_get_category_all_items():
    """
    Test get_category_all_items() returns data[key]['category_value'] for each key
    :assert:
        The info values returned correspond to the value in data
    """
    await _delete_from_configuration()
    data = await _new_category()
    for key in data.keys():
        result = await get_category_all_items(key)
        assert data[key]['category_value']['info']['description'] == result['info']['description']
        assert data[key]['category_value']['info']['type'] == result['info']['type']
        assert data[key]['category_value']['info']['default'] == result['info']['default']
    await _delete_from_configuration()
