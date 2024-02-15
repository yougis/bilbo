from sqlalchemy import create_engine


print("Utils - Connection Imported")

def getEngine(user='usr', pswd='pswd', host='host', port='port', dbase='db_traitement'):
    """
    Returns a SQLAlchemy engine object for connecting to a PostgreSQL database.

    Args:
        user (str): The username for the database connection.
        pswd (str): The password for the database connection.
        host (str): The host address for the database connection.
        dbase (str): The name of the database to connect to.

    Returns:
        sqlalchemy.engine.Engine: The SQLAlchemy engine object.

    """
    connection = f'postgresql://{user}:{pswd}@{host}:{port}/{dbase}'
    return create_engine(connection)
