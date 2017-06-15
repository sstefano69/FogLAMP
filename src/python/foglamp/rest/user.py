import sqlalchemy
import sys
import uuid
import psycopg2
import aiocoap.resource
import logging
import sqlalchemy as sa
# from cbor2 import loads
from sqlalchemy.dialects.postgresql import JSONB
import aiopg.sa
# import foglamp.env as env


_user = sqlalchemy.Table('foglamp.users',sqlalchemy.MetaData(),
                         sqlalchemy.Column("id",sqlalchemy.INTEGER,primary_key=True),
                         sqlalchemy.Column("uid",sqlalchemy.CHAR(80)),
                         sqlalchemy.Column("role_id",sqlalchemy.INTEGER),
                         sqlalchemy.Column("description",sqlalchemy.CHAR(255)),
                         sqlalchemy.Column("pwd",sqlalchemy.CHAR(255)),
                         sqlalchemy.Column("public_key",sqlalchemy.CHAR(255)),
                         sqlalchemy.Column("access_method",sqlalchemy.SMALLINT))

class User:
    @staticmethod
    async def insert(self,*args,**kwargs):
        """
        INSERT rows into table 
            By current table definition each role requires a name that is unique. 
        Args:
            **kwargs: 

        Returns:
            _user.c.id 
        """
        # Expect variables to show as either args or kwargs
        if args is None or len(args) == 0:
            data = kwargs
        else:
            data = args[0]

        stmt = _user.insert().values(data).compile(compile_kwargs={"literal_binds": True})
        async with aiopg.sa.create_engine(env.db_connection_string) as engine:
            async with engine.acquire() as conn:
                try:
                    await conn.execute(str(stmt).replace('"',''))
                except psycopg2.IntegrityError as e:
                    logging.getLogger('user-table').exception("unable to INSERT into table")
                else:
                    try:
                        conn.execute("commit")
                    except psycopg2.IntegrityError as e:
                        logging.getLogger('user-table').exception("unable to COMMIT data")

        return self.__select_id(data) # Return row id that was just inserted

    @staticmethod
    async def delete(self,*args,**kwargs):
        """
        DELETE rows from table based on WHERE conditions
        Args:
            self: 
            *args: 
            **kwargs: 

        Returns:

        """
        # Expect variables to show as either args or kwargs
        if args is None or len(args) == 0:
            data = kwargs
        else:
            data = args[0]
        removed_id = None
        stmt = _user.delete()

        # Prepare WHERE conditions for DELETE stmt
        for column in _user.c:
            for key in list(data.keys()):
                if column.name == key:
                    if removed_id is None:
                        col = column.name
                        removed_id = self.__select_id(col = data[_user.c.name.name])  # id being removed
                    stmt = stmt.where(column == data[key])

        stmt = stmt.compile(compile_kwargs={"literal_binds": True})
        # execute DELETE stmt
        async with aiopg.sa.create_engine(env.db_connection_string) as engine:
            async with engine.acquire() as conn:
                try:
                    await conn.execute(str(stmt).replace('"',''))
                except psycopg2.IntegrityError as e:
                    logging.getLogger('user-table').exception("unable to DELETE from table")
        return removed_id # rows that were remove

    @staticmethod
    async def update(self,value_set=None,where_condition=None):
        """
        Given that **kwargs doesn't necessarily return the values in order, 
            the script execpts value_set (what's being updated) and where_condition (based on)
            as dictionaries. 
        The where condition uses only "=" to at this time. 
        Args:
            value_set: 
            where_condition: 
        Returns:
            _user.c.id
        """
        updated_rows=self.__select_id(where_condition)
        stmt = sqlalchemy.update(_user).values(value_set)

        # WHERE condition
        for column in where_condition:
            where_value = where_condition[column]
            for where_col in _user.c:
                if where_col.name == column:
                    stmt = stmt.where(where_col == where_value)

        stmt = stmt.compile(compile_kwargs={"literal_binds": True})
        async with aiopg.sa.create_engine(env.db_connection_string) as engine:
            async with engine.acquire() as conn:
                try:
                    await conn.execute(str(stmt).replace('"',''))
                except psycopg2.IntegrityError as e:
                    logging.getLogger('user-table').exception("unable to UDATE into table")
        return updated_rows

    @staticmethod
    async def __select_id(self,*args,**kwargs):
        """
        Get row id by _user.c.name in an array format since there could be more than 1 row. 
        Args:
            name: 
        Returns:
            _user.c.id
        """
        # Expect variables to show as either args or kwargs
        if args is None or len(args) == 0:
            data = kwargs
        else:
            data = args[0]
        stmt = sqlalchemy.select([_user.c.id])
        id = []

        # Prepare WHERE conditions for SELECT stmt
        for column in _user.c:
            for name in list(data.keys()):
                if column.name == name:
                    stmt = stmt.where(column == data[column.name])
        stmt = stmt.compile(compile_kwargs={"literal_binds": True})

        # execute  SELECT stmt
        async with aiopg.sa.create_engine(env.db_connection_string) as engine:
            async with engine.acquire() as conn:
                try:
                    result = conn.execute(str(stmt).replace('"',''))
                except psycopg2.IntegrityError as e:
                    logging.getLogger('user-table').exception("unable to retrive IDs from table ")
                else:
                    for i in result.fetchall():
                        id.append(i[0])
        return id


