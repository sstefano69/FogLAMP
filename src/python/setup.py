from setuptools import setup

requires = [
    # coap
    'aiocoap==0.3',
    'cbor2==4.0.0',
    'LinkHeader==0.4.3',
    # postgreSQL
    'aiopg==0.13.0',
    'SQLAlchemy==1.1.10',
    'psycopg2==2.7.1',
    'asyncpg==0.12.0',
    # rest
    'aiohttp==2.1.0',
    'aiohttp_cors==0.5.3',
    'cchardet==2.1.0',
    'httpie==0.9.9',
    'PyJWT==1.5.0',
    # service
    'python-daemon==2.1.2',
]

setup(
    name='FogLAMP',
    version='0.1',
    description='FogLAMP',
    url='http://github.com/foglamp/FogLAMP',
    author='OSIsoft, LLC',
    author_email='info@dianomic.com',
    license='Apache 2.0',
    install_requires=requires,
    packages=['foglamp.translators',
              'foglamp.device',
              'foglamp.device.coap',
              'foglamp.data_purge',
              'foglamp.admin_api',
              'foglamp',
              'foglamp.core.api',
              'foglamp.core',
              ],
    entry_points={
        'console_scripts': [
            'foglamp = foglamp.core.server_daemon:main'
        ],
    },
    zip_safe=False

)