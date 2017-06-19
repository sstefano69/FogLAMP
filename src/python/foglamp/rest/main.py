"""
Date: June 2017 
Description: The following code is an example of how db.py and tables.py work together to INSERT/UPDATE/DELETE
data from database
"""
import db
import tables

def main():
    role = db.Table(table_name=tables.roles,host='192.168.0.182')
    user = db.Table(table_name=tables.users,host='192.168.0.182')

    role.insert(id=1, name='Power User', description='A user with special privileges')
    user.insert(id=1,uid='michael.f',role_id=1,description='',pwd='qwerty',public_key='',access_method=0)
    role.insert(id=2, name='User', description='A user with regular privileges')
    user.update(value_set={'role_id':2},where_condition={'role_id':1})
    role.delete(id=1)


if __name__ == '__main__':
    main()