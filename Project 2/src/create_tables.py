"""
This script is used to drop and create new tables.

Author: Sangwon Lee
Date:2023/02/06
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Execute drop table queries.
        
    Keyword arguments:
    cur -- database cursor
    conn -- database connector
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Execute create table queries.
    
    Keyword arguments:
    cur -- database cursor
    conn -- database connector
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

           
def main():
    """
    Execute functions to create and drop tables.

    Args:
        - input - None
    
    Returns:
        - new tables in the AWS Clusters
    """
    config = configparser.ConfigParser()
    config.read('../config/dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected')
    cur = conn.cursor()
    print()
    print('Cursor Created')

    print()
    print('Dropping existing tables')
    drop_tables(cur, conn)
    print()
    print('Creating tables')
    create_tables(cur, conn)

    conn.close()
    print()
    print('Connection Closed')

if __name__ == "__main__":
    main()