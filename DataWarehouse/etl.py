import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    load_staging_tables - Uses the Redshift COPY command to copy data from S3 buckets into
    the songs_stage and logs_stage tables.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

        
def insert_users(cur, conn):
    """
    insert_users - Special handling for upserting user information.  Currently, it is just needed
    for the 'level' field.  Because Redshift does not support upsert, this needs to be done manually.
    Here ist he procedure:
    1. Get the latest information for all users from the logs_stage table.  This will give us the
    most current value for 'level'.  Copy this data into a temporary table: temp_users
    2. Perform an update of the 'level' field from 'temp_users' into 'users'.
    3. Perform an insert of users in 'temp_users' into 'users' which do not exist.
    """
    
    try:
        # Step 1: Copy latest user information into the temp_users table. NOTE: Redshift does not seem
        # to support TEMP tables, so create a regular table.
        query = """
        SELECT
            LS.userid AS user_id,
            LS.firstname AS first_name,
            LS.lastname AS last_name,
            LS.gender,
            LS.level
        INTO TABLE temp_users
        FROM logs_stage LS
        WHERE (LS.page = 'NextSong') AND LS.ts = (SELECT MAX(B.ts) FROM logs_stage B WHERE LS.userid = B.userid);
        """
        print('\t2.1 Copying latest user information to temp_users')
        cur.execute(query)

        # Step 2: Perform an update of the 'level' field from 'temp_users' into 'users'.
        query = """
        UPDATE users u
        SET level = tu.level
        FROM temp_users tu
        WHERE u.user_id = tu.user_id;
        """
        print('\t2.2 Perform an update of the \'level\' field')
        cur.execute(query)

        # Step 3: Perform an insert of new users from 'temp_users' into 'users'.
        query = """
        INSERT INTO users
        SELECT *
        FROM temp_users
        WHERE temp_users.user_id NOT IN (SELECT DISTINCT user_id FROM users);
        """
        print('\t2.3 Perform an insert of new users from \'temp_users\' into \'users\'')
        cur.execute(query)

        # Drop the temp table
        query = """
        DROP TABLE IF EXISTS temp_users;
        """
        print('\t2.4 Drop the temp table')
        cur.execute(query)

        conn.commit()
    
    except Exception as e:
        conn.rollback()
        raise e

        
def insert_tables(cur, conn):
    """
    insert_tables - Inserts data into the dimension and fact tables.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('1. Calling: load_staging_tables')
    load_staging_tables(cur, conn)
    
    print('2. Calling: insert_users')
    insert_users(cur, conn)
    
    print('3. Calling: insert_tables')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()