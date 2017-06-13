import sqlalchemy
import sys

_user = sqlalchemy.Table('foglamp.users',sqlalchemy.MetaData(),
                         sqlalchemy.Column("id",sqlalchemy.INTEGER,primary_key=True),
                         sqlalchemy.Column("uid",sqlalchemy.CHAR(80)),
                         sqlalchemy.Column("role_id",sqlalchemy.INTEGER),
                         sqlalchemy.Column("description",sqlalchemy.CHAR(255)),
                         sqlalchemy.Column("pwd",sqlalchemy.CHAR(255)),
                         sqlalchemy.Column("public_key",sqlalchemy.CHAR(255)),
                         sqlalchemy.Column("access_method",sqlalchemy.SMALLINT))

class User:
    def __init__(self):
        self.engine = sqlalchemy.create_engine('postgres://foglamp:foglamp@192.168.0.182/foglamp')
        self.conn = self.engine.connect()

    def insert(self,*args,**kwargs):
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
        try:
            self.conn.execute(str(stmt).replace('"',''))
        except:
            sys.exit()
        self.conn.execute("commit")
        return self.select_id(data) # Return row id that was just inserted

    def select_id(self,*args,**kwargs):
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
        try:
            result = self.conn.execute(str(stmt).replace('"', ''))
        except self.conn.Error as e:
            print(e)
            sys.exit()
        # Prepare result as an array of values
        for i in result.fetchall():
            id.append(i[0])
        return id

    def delete(self,*args,**kwargs):
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
                        removed_id = self.select_id(col = data[_user.c.name.name])  # id being removed
                    stmt = stmt.where(column == data[key])

        stmt = stmt.compile(compile_kwargs={"literal_binds": True})
        # execute DELETE stmt
        try:
            result = self.conn.execute(str(stmt).replace('"', ''))
        except self.conn.Error as e:
            print(e)
            sys.exit()
        return removed_id # rows that were remove

    def update(self,value_set=None,where_condition=None):
        """
        Given that **kwargs doesn't necessarily return the values in order, 
            the script execpts value_set (what's being updated) and where_condition (based on)
            as dictionaries. 
        Args:
            value_set: 
            where_condition: 
        Returns:
            _user.c.id
        """
        updated_rows=self.select_id(where_condition)
        stmt = sqlalchemy.update(_user).values(value_set)

        # WHERE condition
        for column in where_condition:
            where_value = where_condition[column]
            for where_col in _user.c:
                if where_col.name == column:
                    stmt = stmt.where(where_col == where_value)

        stmt = stmt.compile(compile_kwargs={"literal_binds": True})
        try:
            result = self.conn.execute(str(stmt).replace('"', ''))
        except self.conn.Error as e:
            print(e)
            sys.exit()

        return updated_rows
