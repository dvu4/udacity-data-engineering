import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list in sql_queries.py
    """
    for query in drop_table_queries:
        print(f'Dropping data by {query}')
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list in sql_queries.py
    """
    for query in create_table_queries:
        print(f'Creating staging, fact and dimension data by {query}')
        cur.execute(query)
        conn.commit()


def main():
    """    
    - Connects to Redshift.  
    
    - Loads data from Amazon S3 to Amazon Redshift  
    
    - Drops (if exists) and Creates the tables. 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print("Connecting to Redshift")
    cur = conn.cursor()

    drop_tables(cur, conn)
    print("Dropping tables if exists!!!")
    
    create_tables(cur, conn)
    print("Creating tables !!!")
    
    conn.close()
    print("Creating table ended !!!")


if __name__ == "__main__":
    main()