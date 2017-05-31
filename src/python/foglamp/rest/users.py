## Create table stmt – password has to be at least 6 characters long.

import aiopg.sa
import foglamp.model.config as config
import sqlalchemy as sa


user_info_table = """
    CREATE TABLE users (
        id SERIAL,
        username text UNIQUE,
        password text, 
        PRIMARY KEY(id, username),
        CHECK(char_length(password) >= 6) 
    ); 
"""
engine = aiopg.sa.create_engine(config.db_connection_string)
conn = engine.acquire()

# POST
def new_user(username, password):
    """
    Create a new user
        if user exists then should return error (currently 1) 
        if user does not exist then create new user (confirm by returning 0) 
    Args:
        username: 
        password: 

    Returns:
        1 - user exists 
        0 - user created
    """
    insrt_stmt = "INSERT INTO users(username, password) VALUES (%s,%s);"
    if find_by_username(username=username) is None:
        conn.execute(insrt_stmt % (username, password))
        conn.execute("commit")
        return 0 # success
    return 1 # user already exists

# GET
def find_by_username(username):
    """
    Find user based on username
    Args:
        username: 

    Returns:
        row - info regarding user if exists 
        None - if user doesn't exist
    """
    slct_stmt="SELECT * FROM users WHERE username = %s;"
    result = cur.execute(slct_stmt % username)
    row = result.fetchone()
    if row:
        return row
    return None

# PUT
def update_user_password(username, old_password, new_password):
    """
    update password information for a given user 
    Args:
        username: 
        old_password: 
        new_password: 
    Returns:
        0 - password updated 
        1 - old_password not equal to current password || user DNE 

    """
    slct_stmt = "SELECT password FROM users WHERE username = %s;"
    updte_stmt = "UPDATE users SET password = %s WHERE username = %s;"
    result= cur.execute(slct_stmt, username)
    row = result.fetchone()
    if row == old_password: # check current password is what the user declared as his old password
        result = cur.execute(updte_stmt % (new_password, username))
        return 0
    return 1

# DELETE
def remove_user(username):
    """
    remove user if exists 
    Args:
        username: 
    Returns:
        0 - user removed 
        1 - user not found
    """
    delt_stmt = "DELETE FROM users WHERE username = %s"
    if find_by_username(username=username):
        cur.execute(delt_stmt % username)
        return 0
    return 1

