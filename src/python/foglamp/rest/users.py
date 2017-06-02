## Create table stmt – password has to be at least 6 characters long.
import aiocoap.resource as resource
import psycopg2
import aiopg.sa
import logging
import sqlalchemy as sa
from cbor2 import loads
import foglamp.model.config as config
import aiocoap

user_info_table = """
    CREATE TABLE user_info (
        id SERIAL,
        username text UNIQUE,
        password text, 
        PRIMARY KEY(id, username),
        CHECK (char_length(password) >= 6)
    ); 
    """

_user_info_table= sa.Table('user_info',sa.MetaData(),
                            sa.Column(name='id',type=sa.types.BIGINT),
                            sa.Column(name='username',type=sa.types.TEXT),
                            sa.Column(name='password',type=sa.types.TEXT))

def check_user_information(conn=None,username="existing_user",password="qwerty"):
    """
    Verify that exists based on name and password
    Args:
        conn: 
        username: 
        password: 

    Returns:

    """
    result=""
    try:
        conn.execute("SELECT password for user_info WHERE username = %s;" % username)
    except psycopg2.IntegrityError as e:
        logging.getLogger('user-data').exception("Unable to find %s" % username)
    else:
        result = conn.fetchone()[0]
    if str(result) == str(password): # This is important becasue according to python 1 != "1"
        return 0
    return 1

class CreateTableUser(resource.Resource):
    def __init__(self):
        super(CreateTableUser, self).__init__()

    async def new_user_table(self, request):
        """
        Create User Table
            CREATE TABLE user_info (
                id SERIAL,
                username text UNIQUE,
                password text, 
                PRIMARY KEY(id, username)    
            ); 
        Args:
            request: 
        Returns:
        """
        async with aiopg.sa.create_engine(config.db_connection_string) as engine:
            async with engine.aquire() as conn:
                try:
                    await conn.execute(user_info_table)
                except psycopg2.IntegrityError as e:
                    logging.getLogger('user-data').exception("Unable to create table user_info")
        return aiocoap.Message(payload=''.encode('utf-8'))

    async def add_new_user(self,request,username,password):
        """
        Add new user 
        Args:
            request: 
            username: 
            password: 

        Returns:
        """
        async with aiopg.sa.create_engine(config.db_connection_string) as engine:
            async with engine.aquire() as conn:
                try:
                    await conn.execute(_user_info_table.insert().values(username=username,password=password))
                except psycopg2.IntegrityError as e:
                    logging.getLogger('user-data').exception("Unable to create new user in user_info")
        return aiocoap.Message(payload=''.encode('utf-8'))

    async def update_user(self,request,username, old_password,new_password):
        """
        Update user password
        Args:
            request: 
            username: 
            old_password: 
            new_password: 

        Returns:
        """
        async with aiopg.sa.create_engine(config.db_connection_string) as engine:
            async with engine.aquire() as conn:
                if check_user_information(conn=conn, username=username, password=old_password) == 0:
                    try:
                        await conn.execute("UPDATE user_info SET password = %s WHERE usernmae = '%s'" % (new_password, username))
                    except psycopg2.IntegrityError as e:
                        logging.getLogger('user-data').exception("Unable to update %s in user_info" % username)
        return aiocoap.Message(payload=''.encode('utf-8'))

    async def remove_user(self,request, username, password):
        """
        Remove user from table based on name and password 
        Args:
            request: 
            username: 
            password: 

        Returns:
        """
        async with aiopg.sa.create_engine(config.db_connection_string) as engine:
            async with engine.aquire() as conn:
                if check_user_information(conn=conn, username=username, password=password) == 0:
                    if check_user_information(conn=conn, username=username, password=password) == 0:
                    try:
                        await conn.execute("DELETE FROM user_info WHERE username = '%s';" % username)
                    except psycopg2.IntegrityError as e:
                        logging.getLogger('user-data').exception("Unable to remove %s in user_info" % username)
            return aiocoap.Message(payload=''.encode('utf-8'))





