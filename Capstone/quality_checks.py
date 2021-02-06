import configparser
import psycopg2
import os


def check_visit_table(cur, conn):
    """
    Runs two data quality checks on the visit table.
    """
    
    # Check 1: Check that the visit table was filled
    cur.execute('SELECT COUNT(*) FROM visit;')
    results = cur.fetchone()

    if results == None:
        print('Warning: Error querying the visit table')
    elif results == 0:
        print('Warning: The visit table had 0 records')
        
    # Check 2: Check that the latest visit is the same between visit and staging_visit
    cur.execute('''
    SELECT MAX(arrival_date) FROM staging_visit;''');
    results_staging = cur.fetchone()
    
    if results_staging == None:
        print('Warning: Error querying the staging_visit table')
        return

    cur.execute('''
    SELECT MAX(arrival_date) FROM visit;
    ''');
    results = cur.fetchone()
    
    if results == None:
        print('Warning: Error querying the visit table')
    else:
        if results[0] != results_staging[0]:
            print('Warning: latest values in visit and staging_visit do not match')
        else:
            print('Quality check on visit table passed')


def check_datetime_table(cur, conn):
    """
    Runs a data quality checks on the datetime table.
    """

    # Check 1: Check that the datetime table was filled
    cur.execute('SELECT COUNT(*) FROM datetime;')
    results = cur.fetchone()

    if results == None:
        print('Warning: Error querying the datetime table')
    elif results == 0:
        print('Warning: The visit table had 0 records')
    else:
        print('Quality check on datetime table passed')


def check_demographics_table(cur, conn):
    """
    Runs two data quality checks on the demographics table.
    """
    
    # Check 1: Check that the demographics table was filled
    cur.execute('SELECT COUNT(*) FROM demographics;')
    results = cur.fetchone()

    if results == None:
        print('Warning: Error querying the demographics table')
    elif results == 0:
        print('Warning: The visit table had 0 records')

    # Check 2: Check that the latest demographics is the same between demographics and staging_demographics
    cur.execute('''
    SELECT MAX(year) FROM staging_demographics;''');
    results_staging = cur.fetchone()
    
    if results_staging == None:
        print('Warning: Error querying the staging_demographics table')
        return

    cur.execute('''
    SELECT MAX(year) FROM demographics;
    ''');
    results = cur.fetchone()
    
    if results == None:
        print('Warning: Error querying the demographics table')
    else:
        if (results[0] != results_staging[0]):
            print('Warning: latest values in demographics and staging_demographics do not match')
        else:
            print('Quality check on demographics table passed')


def check_oilprice_table(cur, conn):
    """
    Runs two data quality checks on the oilprice table.
    """
    
    # Check 1: Check that the oilprice table was filled
    cur.execute('SELECT COUNT(*) FROM oilprice;')
    results = cur.fetchone()

    if results == None:
        print('Warning: Error querying the oilprice table')
    elif results == 0:
        print('Warning: The visit table had 0 records')

    # Check 2: Check that the latest visit is the same between visit and staging_visit
    cur.execute('''
    WITH t1 AS (
    SELECT *
    FROM staging_oilprice
    WHERE staging_oilprice.year = (SELECT MAX(year) FROM staging_oilprice)
    )
    SELECT DISTINCT year, month
    FROM t1
    WHERE t1.month = (SELECT MAX(month) FROM t1);
    ''');
    results_staging = cur.fetchone()
    
    if results_staging == None:
        print('Warning: Error querying the staging_oilprice table')
        return

    cur.execute('''
    WITH t1 AS (
    SELECT *
    FROM oilprice
    WHERE oilprice.year = (SELECT MAX(year) FROM oilprice)
    )
    SELECT DISTINCT year, month
    FROM t1
    WHERE t1.month = (SELECT MAX(month) FROM t1);
    ''');
    results = cur.fetchone()
    
    if results == None:
        print('Warning: Error querying the oilprice table')
    else:
        if (results[0] != results_staging[0]) or (results[1] != results_staging[1]):
            print('Warning: latest values in oilprice and staging_oilprice do not match')
        else:
            print('Quality check on oilprice table passed')


def check_tempbystate_table(cur, conn):
    """
    Runs two data quality checks on the tempbystate table.
    """
    
    # Check 1: Check that the tempbystate table was filled
    cur.execute('SELECT COUNT(*) FROM tempbystate;')
    results = cur.fetchone()

    if results == None:
        print('Warning: Error querying the tempbystate table')
    elif results == 0:
        print('Warning: The visit table had 0 records')

    # Check 2: Check that the latest visit is the same between tempbystate and staging_tempbystate
    cur.execute('''
    WITH t1 AS (
    SELECT *
    FROM staging_tempbystate
    WHERE staging_tempbystate.year = (SELECT MAX(year) FROM staging_tempbystate)
    )
    SELECT DISTINCT year, month
    FROM t1
    WHERE t1.month = (SELECT MAX(month) FROM t1);
    ''');
    results_staging = cur.fetchone()
    
    if results_staging == None:
        print('Warning: Error querying the staging_tempbystate table')
        return

    cur.execute('''
    WITH t1 AS (
    SELECT *
    FROM tempbystate
    WHERE tempbystate.year = (SELECT MAX(year) FROM tempbystate)
    )
    SELECT DISTINCT year, month
    FROM t1
    WHERE t1.month = (SELECT MAX(month) FROM t1);
    ''');
    results = cur.fetchone()
    
    if results == None:
        print('Warning: Error querying the tempbystate table')
    else:
        if (results[0] != results_staging[0]) or (results[1] != results_staging[1]):
            print('Warning: latest values in tempbystate and staging_tempbystate do not match')
        else:
            print('Quality check on tempbystate table passed')


def imp_run_checks():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

    cur = conn.cursor()

    check_visit_table(cur, conn)
    check_datetime_table(cur, conn)
    check_demographics_table(cur, conn)
    check_oilprice_table(cur, conn)
    check_tempbystate_table(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    imp_run_checks()
