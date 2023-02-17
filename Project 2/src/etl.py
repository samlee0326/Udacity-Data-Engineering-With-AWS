"""
This script is used to load and populate analytics tables on AWS RedShift.

Author: Sangwon Lee
Date:2023/02/06
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load staging tables using queries from sql_queries.py.
    
    Keyword arguments:
    cur -- database cursor
    conn -- database connector
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Populate analytics tables on AWS Redshift.
        
    Keyword arguments:
    cur -- database cursor
    conn -- database connector
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Perform ETL process by loading staging tables and populate tables.
    
    Args:
        - input - None
    
    Returns:
        - new tables in the AWS Clusters
    """
    config = configparser.ConfigParser()
    config.read('../config/dwh.cfg')
    print('Connected')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print()
    print('Cursor Created')
    
    print()
    print('Loading staging tables')
    load_staging_tables(cur, conn)

    insert_tables(cur, conn)
    print()
    print('inserted tables')

    conn.close()
    print('Connection Closed')

if __name__ == "__main__":
    main()