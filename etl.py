import configparser
import psycopg2
from sql_queries import copy_table_queries, transform_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function is used once the connection to the Redshift cluster is effective
    It executes SQL instructions based on queries provided in the copy_table_queries list
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def transform_staging_tables(cur, conn):
    """
    This function is used once the connection to the Redshift cluster is effective
    It executes SQL instructions based on queries provided in the transform_table_queries list
    """
    for query in transform_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function is used once the connection to the Redshift cluster is effective
    It executes SQL instructions based on queries provided in the insert_table_queries list
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This is the process that coordinates operations to populate the data model on the Redshift cluster
    Basically it performs high level DML operations in sequence:
    - connect to the Redshift cluster on AWS using the configuration provided
    - call the load function to load data in staging tables
    - call the insert function to insert data in DWH tables (staging + dimension & fact tables)
    - close the connection
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    print("Connected")

    cur = conn.cursor()

    # Extract part
    load_staging_tables(cur, conn)
    print("Staging tables loaded [EXTRACT]")
    # Transform part
    transform_staging_tables(cur, conn)
    print("Staging tables transformed [TRANSFORM]")
    # Load part
    insert_tables(cur, conn)
    print("Fact and dimension tables loaded [LOAD]")

    conn.close()


if __name__ == "__main__":
    main()
