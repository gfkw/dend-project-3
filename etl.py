import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Read all the source files from S3 and load into staging tables.

    Keyword arguments:
    cur -- the connected cursor
    conn -- connection string to Postgres server
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Read the staging tables and load into star schema tables.

    Keyword arguments:
    cur -- the connected cursor
    conn -- connection string to Postgres server
    """

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """This program will extract all files from song and log directories in S3, load the staging area, perform data transformations and load into fact and dimesions tables.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
