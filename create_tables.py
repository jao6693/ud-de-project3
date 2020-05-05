import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function is used once the connection to the Redshift cluster is effective
    It executes SQL instructions based on queries provided in the drop_table_queries list
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function is used once the connection to the Redshift cluster is effective
    It executes SQL instructions based on queries provided in the create_table_queries list
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This is the process that coordinates operations to build the data model on the Redshift cluster
    Basically it performs high level DDL operations in sequence:
    - connect to the Redshift cluster on AWS using the configuration provided
    - call the drop function to drop existing tables (staging + DWH)
    - call the create function to create tables (staging + DWH)
    - close the connection
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    print("Connected")

    cur = conn.cursor()

    drop_tables(cur, conn)
    print("Tables dropped")
    create_tables(cur, conn)
    print("Tables created")

    conn.close()
    print("Connection closed")


if __name__ == "__main__":
    main()
