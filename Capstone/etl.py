import configparser
import psycopg2
import os
from sql_queries import staging_copy, truncate_table_queries, delete_duplicate_queries, insert_table_queries
from sql_queries import visit_key, demographics_key, oilprice_key, tempbystate_key


def load_staging_tables(cur, conn):
    """
    Copies Parquet files from S3 into staging tables in Redshift.
    """
    # Copy Parquet files to staging tables
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Truncate tables
    for query in truncate_table_queries:
        cur.execute(query)
        conn.commit()

    # Get the bucket name
    s3_bucket = config.get('S3', 'S3_BUCKET')

    # Iterate over the staging tables and copy data into them
    keys = [('staging_visit', visit_key), ('staging_demographics', demographics_key), ('staging_oilprice', oilprice_key), ('staging_tempbystate', tempbystate_key)]

    for key in keys:
        query = staging_copy.format(key[0], os.path.join(s3_bucket, key[1] + '/'), config.get('IAM_ROLE', 'ARN'))
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Upserts data from the staging tables into the tables in the data warehouse.
    """
    # 1. Upsert: first delete duplicates in the staging table
    for query in delete_duplicate_queries:
        cur.execute(query)
        conn.commit()

    # 2. Upsert: then insert the rows from the staging table into the database tables
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def imp_update_tables():
    """
    The implementation for updating tables in the database.  This can be called
    from main() in this file or from a Jupyter notebook.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

    cur = conn.cursor()

    # Truncate the staging tables, and then load the parquet files into the staging tables
    load_staging_tables(cur, conn)

    # Update the fact and dimension tables from the staging tables
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    imp_update_tables()
