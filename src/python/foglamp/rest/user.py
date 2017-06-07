"""
The User correlates to the following list of tables: 
-- users:                           provide information regarding a given user 
-- user_login:                      provide information regarding logging into the system 
-- roles:                           provide information regarding a given role 
-- user_asset_permissions:          provide information regarding which assets user has access to          
-- user_resource_permissions:       provide information regarding which resouces user has access to
-- role_asset_permissions:          provide information regarding which assets a role has access to
-- role_resource_permission:        provide information regarding which resouces a role has access to

Questions: 
1. Can a user with a given role has different permissions than his/her role? 
2. for users.uid is that unique, or can 2 users have the same name 

Relationship 
users.id is referenced by: 
- user_asset_permissions.user_id 
- user_logins.user.id 
- user_resource_permissions.user_id 

roles.id is refrenced by: 
- users.role_id
"""

import aiocoap.resource as resource
import psycopg2
import aiopg.sa
import logging
import sqlalchemy as sa
from cbor2 import loads
import foglamp.model.config as config
import aiocoap


# Validate SQL - https://stackoverflow.com/questions/20718469/validation-in-sqlalchemy

class TestUser:
    def __init__(self):
        self.user=User()

    def test_insert_role(self):
        """
        Validate that an Insert works 
        Returns:
        """
        # should fail because table not expected
        self.user.insert_row(table='role',columnValues={'id':1234,'name':'qa','description':'test product'})
        # should fail because column not expected
        self.user.insert_row(table='role', column={'id': 1234, 'name': 'qa', 'description': 'test product'})
        # should pass
        self.user.insert_row(tableName='role', columnValues={'id': 1234, 'name': 'qa', 'description': 'test product'})

    def test_update_row(self):
        """
        validate that an update works 
        Returns:
        """
        # Test 1 for 1
        self.user.update_row(tableName='role',updateValues={'description': 'create new tests'},whereValues={'id': 1234})
        # Test 2 for 1
        self.user.update_row(tableName='role',updateValues={'name':'test developer','description':'create new tests'},
                             whereValues={'id':1234})
        # Test 1 for 2
        self.user.update_row(tableName='role',updateValues={'description': 'create new tests'},
                             whereValues={'id': 1234,'name':'qa'})

    def test_remove_row(self):
        # only table name and data
        self.user.remove_row(tableName='role',whereValue={'id':1234})
        # only table name
        self.test_remove_row(tableName='role')

    def test_select_all_roles(self):
        """
        validate that SELECT *
        """
        # should fail because table not expected
        self.user.select_all(table='role')
        # should pass
        self.user.select_all(tableName='role')

class User:
    def __init__(self):
        super(User, self).__init__()

    async def insert_row(self,**args,**kwargs):
        """
        based on the information provided by user insert rows 
            User should provide: tableName, and columnValues which is a dict containing both column [key] and values
        Args:
            **kwargs: 
        Returns:
            aiocoap.Message
        """
        tableName = args[0]

        values = {}
        for key in kwargs.keys():
            values[key] = kwargs[key]

        async with aiopg.sa.create_engine(config.db_connection_string) as engine:
            async with engine.aquire() as conn:
                try:
                    await sa.sql.expression.insert(tableName, values=values, inline=False, bind=None, prefixes=None,
                                                   returning=None, return_defaults=False)
                except psycopg2.IntegrityError as e:
                    logging.getLogger('user-data').exception("Unable to execute query against user/role data")
        return aiocoap.Message(payload=''.encode('utf-8'))

    async def update_row(self,**args,**kwargs):
        """
        Unlike the INSERT and DELETE which have **kwagers that all go to the same SQLAlchemy variable, the update
        has one that goes to the UPDATE VALUE condition, and another to the WHERE condition. As such, while I'm using
        kwargs, they are really expecting two method variables 'updateColumn','whereColumn'. In each variable a 
        dictionary of values corresponding to the change will be placed. 
        Command example: 
            user.update_row(tableName,updateColumn={column:'value',column:'value',column:'value'},
                            whereCondition={column:'value',column:'value',column:'value'}) 


        Update specific row(s) for table based on WHERE condition
            User should provide: tableName, updateColumn dict containing updating column name [key] and value,
                                  whereColumn containing column name [key] and value 
        Args:
            **args
            **kwargs: 
        Returns:
            aiocoap.Message(payload=''.encode('utf-8'))
        """
        tableName=args[0]
        command = kwargs
        stmt = "UPDATE {} SET {} WHERE {}"
        updateColumn={}
        whereColumn={}

        # Check for appropriate information
        if sorted(command.keys()) != sorted(['updateColumn','whereColumn']):
            print ("Error Unexpected Key")
            return

        #Prepare update statement
        for column in kwargs['updateColumn'].keys():
            updateColumn[column]=kwargs['updateColumn'][column]

        for column in command['whereColumn'].keys():
            whereColumn[column]=kwargs['whereColumn'][column]

        # Execute statement
        async with aiopg.sa.create_engine(config.db_connection_string) as engine:
            async with engine.aquire() as conn:
                try:
                    await sa.sql.expression.update(tableName, whereclause=whereColumn, values=updateColumn,
                                                           inline=False, bind=None, prefixes=None, returning=None,
                                                           return_defaults=False, preserve_parameter_order=False, **dialect_kw)¶

                except psycopg2.IntegrityError as e:
                    logging.getLogger('user-data').exception("Unable to execute query against user/role data")
        return aiocoap.Message(payload=''.encode('utf-8'))


    async def remove_row(self,**args,**kwargs):
        tableName = args[0]

        values = {}
        for key in kwargs.keys():
            values[key] = kwargs[key]

        async with aiopg.sa.create_engine(config.db_connection_string) as engine:
            async with engine.aquire() as conn:
                try:
                    await sa.sql.expression.delete(tableName, values=values, inline=False, bind=None, prefixes=None,
                                                   returning=None, return_defaults=False)
                except psycopg2.IntegrityError as e:
                    logging.getLogger('user-data').exception("Unable to execute query against user/role data")
        return aiocoap.Message(payload=''.encode('utf-8'))


