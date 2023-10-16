import pymysql
from sqlalchemy import create_engine
import time
import sys
import os
from tqdm import tqdm
import math
import json
import multiprocessing
import pandas as pd

database = os.environ.get('database', 'scigene_visualization_field')
# numOfTopAuthors = os.environ.get('numOfTopAuthors', 1100)
# numOfTopAuthors = int(numOfTopAuthors)

multiproces_num = 20
filterCondition = os.environ.get('filterCondition', f"PaperCount_field > 20")
# select exact top field authors


# 对于 authorID 的限制
# with open('out/test.txt', 'r') as f:
#     authorID_list = f.read().split()
# authorID_list = ['2147343253', '2076420186', '2122885999', 
#                  '2003408012', '2762167099', '2158935544']
# authorID_list = ['3206897746']

# ids_string = ', '.join(map(str, authorID_list))
# filterCondition = f"authorID IN ({ids_string})"
# print(filterCondition)
# authorRank > 1000 and authorRank <= {numOfTopAuthors};

def create_connection(database):
    conn = pymysql.connect(host='localhost',
                                user='root',
                                password='root',
                                db=database,
                                charset='utf8')
    return conn, conn.cursor()


def init_connection(database):
    try:
        return create_connection(database)
    except:
        # Connect to the MySQL server without selecting a database
        conn = pymysql.connect(host='localhost', user='root', password='root')
        cursor = conn.cursor()
        cursor.execute(f"SHOW DATABASES LIKE '{database}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {database}")
        conn.commit()

        return create_connection(database)
    
conn, cursor = init_connection(database)
engine = create_engine(f"mysql+pymysql://root:root@192.168.0.140:3306/{database}?charset=utf8")


# 当你使用pymysql直接创建的连接，它返回的是一个原生的MySQL连接，
# 而pandas的to_sql方法期望一个SQLAlchemy引擎作为其连接参数。

# 为了使用pandas的to_sql方法与MySQL数据库，你需要使用SQLAlchemy来创建一个连接引擎。
# to_sql需要更多的功能，如检查表是否存在、创建表、插入数据等。
# 这些功能在不同的数据库中可能会有所不同，因此pandas依赖于SQLAlchemy来提供这种数据库无关的接口

# pandas的read_sql_query和read_sql函数设计得更为灵活，
# 它们可以接受原生的数据库连接（如通过pymysql创建的连接）或SQLAlchemy引擎作为其连接参数。
# 这是为了方便用户从各种数据库中读取数据。


def execute(sql, cursor=cursor):
    for _sql in sql.split(';'):
        _sql = _sql.strip()
        if _sql == '':
            continue
        print('* execute', _sql)
        t = time.time()
        cursor.execute(_sql)
        conn.commit()
        print('[time cost: ', time.time()-t, ']')


def executeFetch(sql, cursor=cursor):
    sql = sql.strip()
    print('* executeFetch', sql)
    t = time.time()
    cursor.execute(sql)
    rows = cursor.fetchall()
    print('[time cost: ', time.time()-t, ']')
    return rows


# authors_rows = executeFetch(f"""
# select authorID, name, PaperCount_field 
#     from {database}.authors_field
#     where {filterCondition} order by authorID;""")
# authorID_list = [row[0] for row in authors_rows]

top_field_authors_path = f'out/{database}/top_field_authors.csv'

if os.path.exists(top_field_authors_path):
    print('top_field_authors.csv exists')
    top_field_authors_df = pd.read_csv(top_field_authors_path)
else:
    print('top_field_authors.csv not exists')
    top_field_authors_df = pd.read_sql(f"""select * from authors_field
        where {filterCondition}""", conn)
    top_field_authors_df.to_csv(top_field_authors_path, index=False)

top_field_authors_df['authorID'] = top_field_authors_df['authorID'].astype(str)
authorID_list = top_field_authors_df['authorID'].tolist()


def try_execute(sql, cursor=cursor):
    try:
        cursor.execute(sql)
    except:
        pass
    conn.commit()

