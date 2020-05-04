import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 buckets to Redshift using queries in `copy_table_queries` list in sql_queries.py
    """
    for query in copy_table_queries:
        print(f'Loading data by {query}')
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from staging tables to the fact and dimension tables using queries in `insert_table_queries` list in sql_queries.py
    """
    for query in insert_table_queries:
        print(f'Transforming data by {query}')
        cur.execute(query)
        conn.commit()


def main():
    """    
    - Connects to Redshift.  
    
    - Loads data from Amazon S3 to Amazon Redshift  
    
    - Inserts data from staging tables to the fact and dimension tables
    
    - Finally, closes the connection. 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print("Connecting to Redshift")
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print("Loading staging tables")
    
    insert_tables(cur, conn)
    print("Transforming staging tables")
    
    conn.close()
    print("Finishing ETL")


if __name__ == "__main__":
    main()