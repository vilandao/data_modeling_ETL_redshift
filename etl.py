import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, get_rows_table_queries


def load_staging_tables(cur, conn):
    """
    Load staging tables stated in sql.queries.py
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data into fact and dimensional tables stated in sql.queries.py
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

        
def get_results(cur, conn):
    """
    Get count of rows in each table stated in sql.queries.py to confirm table creation
    """
    for query in get_rows_table_queries:
        print('Running ' + query)
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print("   ", row)

            
def main():
    """
    Connect to the database, run the 3 functions above to load staging tables, 
    insert data into fact and dimensional tables, and check the result
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    get_results(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()