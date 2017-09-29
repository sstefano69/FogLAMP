"""
py-test fixtures that are available for all the modules of tests
"""
import os
import time
from subprocess import call
import pytest


@pytest.fixture(scope="module", autouse=True)
def setup_ddl(request):
    """
    Module level fixture that is called for each test file
        Before the tests starts, it recreates the foglamp ddl
    Fixture called by default (autouse=True)
    """
    yield os.system("psql < `locate foglamp_ddl.sql | grep 'FogLAMP/src/sql'` > /dev/null 2>&1")


@pytest.fixture(scope="module", autouse=False)
def start_foglamp(request):
    """
    Module level fixture that is called for each test file
        Before the tests starts, it starts all foglamp services and
        stop all foglamp services after all tests of module are complete
    Fixture not called by default (autouse=False)
    Use this fixture if you want to use foglamp services
    """
    call(["foglamp", "start"])
    time.sleep(4)
    yield
    call(["foglamp", "stop"])
    time.sleep(4)


@pytest.fixture(scope="module", autouse=False)
def create_init_data(request):
    """
    Module level fixture that is called for each test file
        Before the tests starts, it creates the init data
    Fixture not called by default (autouse=False)
    Use this fixture if you want to use init data
    """
    os.system("psql < `locate foglamp_init_data.sql | grep 'FogLAMP/src/sql'` > /dev/null 2>&1")


@pytest.fixture(scope="session", autouse=True)
def session_cleanup(request):
    """
    Session level fixture that is called for each test session
        Do nothing before the test session start,
        after the test session is over, recreate ddl and populate the init data
    Fixture called by default (autouse=True)
    """
    yield
    os.system("psql < `locate foglamp_ddl.sql | grep 'FogLAMP/src/sql'` > /dev/null 2>&1")
    os.system("psql < `locate foglamp_init_data.sql | grep 'FogLAMP/src/sql'` > /dev/null 2>&1")
