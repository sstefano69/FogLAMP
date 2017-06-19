"""
Date: June 2017 
Description: The following piece of code is a non-asyncio example of communicating with the database. 
                As of now, it contains examples for INSERT/UPDATE/DELETE but still requires work on
                SELECT, migration to async format, and unit-testing. 
            Look at coap/sensor_values for an example on how to do the asyncio instead of the current
            psql connect being currently used. 
"""
import sqlalchemy
import sys

import tables

class Table:
    def __init__(self,table_name=None,host='localhost',db='foglamp'):
        """
        The following sets the table name, and connects to the database. 
            The "assumption" (for now) is that all the engineers use an identical VM with the
             exception of the host ip. 
        Args:
            table_name: 
            host: 
            db: 
        """
        self.table_name=table_name
        print(self.table_name)
        self.engine = sqlalchemy.create_engine('postgres://foglamp:foglamp@%s/foglamp' % host)
        self.conn = self.engine.connect()


    def insert(self,*args,**kwargs):
        """
        INSERT rows into table  
        Args:
            **kwargs: 
        Returns: 
            As of now now there is suppose to be some sort of ID being returned, however, am still not 100% which
            one, since the row ID for each column could have a different name
        """
        # Expect variables to show as either args or kwargs
        if args is None or len(args) == 0:
            data = kwargs
        else:
            data = args[0]


        stmt = self.table_name.insert().values(data).compile(compile_kwargs={"literal_binds": True})
        stmt = str(stmt).replace('"', '')
        try:
            result = self.conn.execute(stmt)
        except self.conn.Error as e:
            print(e)
            sys.exit()
        else:
            return result

    def delete(self,*args,**kwargs):
        """
        DELETE rows from table based on WHERE conditions
        Args:
            *args: 
            **kwargs: 
        Returns:
             As of now now there is suppose to be some sort of ID being returned, however, am still not 100% which
            one, since the row ID for each column could have a different name
        """
        stmt = self.table_name.delete()
        if args is None or len(args) == 0:
            data = kwargs
        else:
            data = args[0]

        for column in self.table_name.c:
            # Prepare WHERE conditions for DELETE stmt
            for key in list(data.keys()):
                if column.name == key:
                    col = column.name
                    stmt = stmt.where(column == data[key])

        stmt = stmt.compile(compile_kwargs={"literal_binds": True})
        try:
            self.conn.execute(str(stmt).replace('"',''))
        except self.conn.Error as e:
            print(e)
            sys.exit()

    def update(self,value_set=None,where_condition=None):
        """
        Given that **kwargs doesn't necessarily return the values in order, 
            the script execpts value_set (what's being updated) and where_condition (based on)
            as dictionaries. 
        The where condition uses only "=" to at this time. 
        Args:
            value_set: 
            where_condition: 
        Returns:
             As of now now there is suppose to be some sort of ID being returned, however, am still not 100% which
            one, since the row ID for each column could have a different name
        """
        stmt = sqlalchemy.update(self.table_name).values(value_set)
        # WHERE condition
        for column in where_condition:
            where_value = where_condition[column]
            for where_col in self.table_name.c:
                if where_col.name == column:
                    stmt = stmt.where(where_col == where_value)

        stmt = stmt.compile(compile_kwargs={"literal_binds": True})
        try:
            self.conn.execute(str(stmt).replace('"',''))
        except self.conn.Error as e:
            print(e)
            sys.exit()

