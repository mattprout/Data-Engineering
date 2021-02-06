import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Iterates over all the tables in the database and drops each one.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Iterates over the list of tables to create and creates each one.
    The tables consist of two staging tables (songs_stage and logs_stage), four
    dimension tables users, songs, artists, time) and a fact table (songplays).
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # First drop the tables (if they exist)
    drop_tables(cur, conn)
    
    # Then create the tables
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()