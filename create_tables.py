import configparser
import psycopg2
from sql_queries import create_schema_queries, create_table_queries, drop_table_queries


def create_schemas(cur, conn):
    """This function will call sql_queries.py to create the stage and star schemas.

    Keyword arguments:
    cur -- the connected cursor
    conn -- connection string to Redshift
    """

    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()


def drop_tables(cur, conn):
    """This function will call sql_queries.py to drop the staging and star schema tables if they already exist.

    Keyword arguments:
    cur -- the connected cursor
    conn -- connection string to Redshift
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """This function will call sql_queries.py to create the staging and star schema tables.

    Keyword arguments:
    cur -- the connected cursor
    conn -- connection string to Redshift
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """This program will execute a sequence of functions to create the schemas and tables.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    create_schemas(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
