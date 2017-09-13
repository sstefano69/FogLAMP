from foglamp.configuration_manager import (create_category, _configuration_tbl)
import aiopg
import pytest
import sqlalchemy as sa

__author__ = "Ori Shadmon"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_CERTIFICATE = """-----BEGIN CERTIFICATE-----
MIICgjCCAesCAgPoMA0GCSqGSIb3DQEBBQUAMIGIMQswCQYDVQQGEwJVSzEPMA0G
A1UECAwGTG9uZG9uMQ8wDQYDVQQHDAZMb25kb24xGjAYBgNVBAoMEUR1bW15IENv
bXBhbnkgTHRkMRowGAYDVQQLDBFEdW1teSBDb21wYW55IEx0ZDEfMB0GA1UEAwwW
T3Jpcy1NYWNCb29rLVByby5sb2NhbDAeFw0xNzA5MTMwMTAxNTNaFw0yNzA5MTEw
MTAxNTNaMIGIMQswCQYDVQQGEwJVSzEPMA0GA1UECAwGTG9uZG9uMQ8wDQYDVQQH
DAZMb25kb24xGjAYBgNVBAoMEUR1bW15IENvbXBhbnkgTHRkMRowGAYDVQQLDBFE
dW1teSBDb21wYW55IEx0ZDEfMB0GA1UEAwwWT3Jpcy1NYWNCb29rLVByby5sb2Nh
bDCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEA4XULS0OFgOHWOBEN84ahEb31
9If99Z92gNIGadjfgSw7jZj3KnGrcA4qtLeE+gCqMvuPpITBNoU0kcmE225uSMMg
gSMRuYHp28WX+5VxLhOeeBOHf6h+tXA1nXeHOW67kKqu8fa88sOK3rxj5zlrAKLK
w+XLRCbRQP3CjtGDRq0CAwEAATANBgkqhkiG9w0BAQUFAAOBgQATF6/5bw+jlS+S
iPNp/cpxYtsAs743lGVC9OoY3Aq9YP1Lep+hCzQ1fF2Vz3O3IFsVYh5Y/pVWxZez
zRI+8IpiFbfYIvmIecfUZ4kxFOG3gfB7gz0lSB7ZiI2npncvj6qN+tifS+vACTAy
qBi7lidzWsVmOVRVQazN1CgOir19vg==
-----END CERTIFICATE-----"""

_connection_string = "dbname='foglamp'"

data = {'boolean': {'category_description': 'boolean type',
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
                         'default':'x509_certificate.cer'}}},
        'password': {'category_description': 'Password Type',
                     'category_value': {
                         'info': {
                             'description': "Password Type with default ''",
                             'type': 'password',
                             'default': ''}}}
        }


async def _delete_from_configuration():
    try:
        async with aiopg.sa.create_engine(_connection_string) as engine:
            async with engine.acquire() as conn:
                await conn.execute("DELETE FROM foglamp.configuration")
    except Exception:
        print("DELETE Failed")
        raise

async def _new_category():
    for name in data.keys():
        await create_category(category_name=name, category_description=data[name]['category_description'],
                              category_value=data[name]['category_value'])


@pytest.mark.asyncio
async def test_new_category():
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

    await _new_category()
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