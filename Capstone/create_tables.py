import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_tables(cur, conn):
    """
    Iterates over the list of tables to create each one.
    The tables consist of staging tables, dimension tables, and a fact table.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
        
def drop_tables(cur, conn):
    """
    Iterates over the list of tables to drop each one.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
        
def imp_create_tables():
    """
    The implementation for creating tables in the database.  This can be called
    from main() in this file or from a Jupyter notebook.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Drop the tables (uncomment if necessary)
    #drop_tables(cur, conn)

    # Create the tables
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    imp_create_tables()
