""" 
Date: June 2017 
Description: The following is a file containing all the tables in terms of column_name, data_type, and PRIMARY KEY
        It is intended to be used instead of a dict of tables. This file assumes that the tables already exists. 
        That said, if the tables get fully completed (IE convert sql/foglamp_ddl.sql from SQL to SQLAlchemy format) 
        then there can be an additional method in db.py that actually generates the tables inside the database. 
"""
import sqlalchemy
import sqlalchemy.dialects.postgresql as postgresql

log_codes = sqlalchemy.Table('foglamp.log_codes',sqlalchemy.MetaData(),
        sqlalchemy.Column('code',sqlalchemy.CHAR(5),primary_key=True),
        sqlalchemy.Column('description',sqlalchemy.CHAR(80)))

log = sqlalchemy.Table('foglamp.log',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.BIGINT,primary_key=True),
        sqlalchemy.Column('code',sqlalchemy.CHAR(5)),
        sqlalchemy.Column('level',sqlalchemy.SMALLINT),
        sqlalchemy.Column('log',postgresql.JSONB),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

asset_status = sqlalchemy.Table('foglamp.asset_status',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('descriprion',sqlalchemy.CHAR(255)))

asset_types = sqlalchemy.Table('foglamp.asset_types',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('description',sqlalchemy.CHAR(255)))

assets = sqlalchemy.Table('foglamp.assets',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('code',sqlalchemy.CHAR(50)),
        sqlalchemy.Column('description',sqlalchemy.CHAR(255)),
        sqlalchemy.Column('type_id',sqlalchemy.INTEGER),
        sqlalchemy.Column('address',postgresql.INET), # NOT 100% this would work due to data-type
        sqlalchemy.Column('status_id',sqlalchemy.INTEGER),
        sqlalchemy.Column('properties',postgresql.JSONB),
        sqlalchemy.Column('has_readings',sqlalchemy.Boolean),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

asset_status_changes = sqlalchemy.Table('foglamp.asset_status_changes',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.BIGINT,primary_key=True),
        sqlalchemy.Column('asset_id',sqlalchemy.INTEGER),
        sqlalchemy.Column('status_id',sqlalchemy.INTEGER),
        sqlalchemy.Column('log',postgresql.JSONB),
        sqlalchemy.Column('start_ts',sqlalchemy.TIMESTAMP),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

links = sqlalchemy.Table('foglmap.links',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.INTEGER),
        sqlalchemy.Column('asset_id',sqlalchemy.INTEGER),
        sqlalchemy.Column('properties',postgresql.JSONB),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

asset_links = sqlalchemy.Table('foglamp.asset_links',sqlalchemy.MetaData(),
        sqlalchemy.Column('link_id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('asset_id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

asset_message_status = sqlalchemy.Table('foglamp.asset_message_status',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('description',sqlalchemy.CHAR(255)))

asset_messages = sqlalchemy.Table('foglamp.asset_messages',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.BIGINT,primary_key=True),
        sqlalchemy.Column('asset_id',sqlalchemy.INTEGER),
        sqlalchemy.Column('status_id',sqlalchemy.INTEGER),
        sqlalchemy.Column('message',postgresql.JSONB),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))


readings = sqlalchemy.Table('foglamp.readings',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.BIGINT, primary_key=True),
        sqlalchemy.Column('asset_code',sqlalchemy.CHAR(50)),
        sqlalchemy.Column('read_key',postgresql.UUID), # NOT 100% this would work due to data-type
        sqlalchemy.Column('reading',postgresql.JSONB),
        sqlalchemy.Column('user_ts',sqlalchemy.TIMESTAMP),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

destinations = sqlalchemy.Table('foglamp.destinations',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('description',sqlalchemy.CHAR(255)),
        sqlalchemy.Column('properties',postgresql.JSONB),
        sqlalchemy.Column('address',postgresql.INET),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

streams = sqlalchemy.Table('foglamp.streams',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('destination_id',sqlalchemy.INTEGER),
        sqlalchemy.Column('description',sqlalchemy.CHAR(255)),
        sqlalchemy.Column('properties',postgresql.JSONB),
        sqlalchemy.Column('filters',postgresql.JSONB),
        sqlalchemy.Column('reading_id',sqlalchemy.BIGINT),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

stream_assets = sqlalchemy.Table('foglamp.stream_assets',sqlalchemy.MetaData(),
        sqlalchemy.Column('stream_id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('asset_id',sqlalchemy.INTEGER,primary_key=True))

stream_status = sqlalchemy.Table('foglamp.stream_status',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.INTEGER, primary_key=True),
        sqlalchemy.Column('description',sqlalchemy.CHAR(255)))

stream_log = sqlalchemy.Table('foglamp.stream_log',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.BIGINT,primary_key=True),
        sqlalchemy.Column('stream_id',sqlalchemy.INTEGER),
        sqlalchemy.Column('status_id',sqlalchemy.INTEGER),
        sqlalchemy.Column('reading_id',sqlalchemy.INTEGER),
        sqlalchemy.Column('log',postgresql.JSONB),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

configuration = sqlalchemy.Table('foglamp.configuration',sqlalchemy.MetaData(),
        sqlalchemy.Column('key',sqlalchemy.CHAR(5), primary_key=True),
        sqlalchemy.Column('value',postgresql.JSONB),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

configuration_changes = sqlalchemy.Table('foglamp.configuration_changes',sqlalchemy.MetaData(),
        sqlalchemy.Column('key',sqlalchemy.CHAR(5)),
        sqlalchemy.Column('configuration_ts',sqlalchemy.TIMESTAMP),
        sqlalchemy.Column('configuration_value',postgresql.JSONB),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

clean_rules = sqlalchemy.Table('foglamp.clean_rules',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.INTEGER, primary_key=True),
        sqlalchemy.Column('type',sqlalchemy.CHAR(3)),
        sqlalchemy.Column('object_id',sqlalchemy.BIGINT),
        sqlalchemy.Column('rule',postgresql.JSONB),
        sqlalchemy.Column('rule_check',postgresql.JSONB),
        sqlalchemy.Column('status',sqlalchemy.SMALLINT),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

cloud_send_rules = sqlalchemy.Table('foglamp.cloud_send_rules',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('stream_id',sqlalchemy.INTEGER),
        sqlalchemy.Column('rule',postgresql.JSONB),
        sqlalchemy.Column('rule_check',postgresql.JSONB),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

resources = sqlalchemy.Table('foglamp.resources',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.BIGINT,primary_key=True),
        sqlalchemy.Column('code',sqlalchemy.CHAR(10)),
        sqlalchemy.Column('description',sqlalchemy.CHAR(255)))

roles = sqlalchemy.Table('foglamp.roles',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('name',sqlalchemy.CHAR(25)),
        sqlalchemy.Column('description',sqlalchemy.CHAR(255)))

role_resource_permission = sqlalchemy.Table('foglamp.role_resource_permission', sqlalchemy.MetaData(),
        sqlalchemy.Column('role_id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('resource_id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('access',postgresql.JSONB))

role_asset_permissions = sqlalchemy.Table('foglamp.role_asset_permissions',sqlalchemy.MetaData(),
        sqlalchemy.Column('role_ide',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('asset_id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('access',postgresql.JSONB))

users = sqlalchemy.Table('foglamp.users',sqlalchemy.MetaData(),
        sqlalchemy.Column("id",sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column("uid",sqlalchemy.CHAR(80)),
        sqlalchemy.Column("role_id",sqlalchemy.INTEGER),
        sqlalchemy.Column("description",sqlalchemy.CHAR(255)),
        sqlalchemy.Column("pwd",sqlalchemy.CHAR(255)),
        sqlalchemy.Column("public_key",sqlalchemy.CHAR(255)),
        sqlalchemy.Column("access_method",sqlalchemy.SMALLINT))

user_logins = sqlalchemy.Table('foglamp.user_logins',sqlalchemy.MetaData(),
        sqlalchemy.Column('id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('user_id',sqlalchemy.INTEGER),
        sqlalchemy.Column('ip',postgresql.INET),
        sqlalchemy.Column('ts',sqlalchemy.TIMESTAMP))

user_resource_permissions = sqlalchemy.Table('foglamp.user_resource_permissions',sqlalchemy.MetaData(),
        sqlalchemy.Column('user_id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('resource_id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('access',postgresql.JSONB))

user_asset_permissions = sqlalchemy.Table('foglamp.user_asset_permissions',sqlalchemy.MetaData(),
        sqlalchemy.Column('user_id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('asset_id',sqlalchemy.INTEGER,primary_key=True),
        sqlalchemy.Column('access',postgresql.JSONB))