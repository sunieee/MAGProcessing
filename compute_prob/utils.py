import pymysql
from sqlalchemy import create_engine
import time
import sys
import os
from tqdm import tqdm
import math
import json
import multiprocessing
import datetime
import pandas as pd
from collections import defaultdict

database = os.environ.get('database', 'scigene_visualization')
multiproces_num = 20
topN = os.environ.get('topN', 5000)
topN = int(topN)
suffix = 'ARC' if database.count("acl_anthology") else 'field'

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
                                user=os.environ.get('user'),
                                password=os.environ.get('password'),
                                db=database,
                                charset='utf8')
    return conn, conn.cursor()


def init_connection(database):
    try:
        return create_connection(database)
    except:
        # Connect to the MySQL server without selecting a database
        conn = pymysql.connect(host='localhost', user=os.environ.get('user'), 
                               password=os.environ.get('password'))
        cursor = conn.cursor()
        cursor.execute(f"SHOW DATABASES LIKE '{database}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {database}")
        conn.commit()
        return create_connection(database)
    
conn, cursor = init_connection(database)
userpass = f'{os.environ.get("user")}:{os.environ.get("password")}'
engine = create_engine(f"mysql+pymysql://{userpass}@192.168.0.140:3306/{database}?charset=utf8")


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
# select authorID, name, PaperCount 
#     from {database}.authors
#     where {filterCondition} order by authorID;""")
# authorID_list = [row[0] for row in authors_rows]

def try_execute(sql, cursor=cursor):
    try:
        cursor.execute(sql)
    except:
        pass
    conn.commit()

cursor.execute(f'select hIndex_{suffix} from authors_{suffix} order by hIndex_{suffix} desc limit 1 offset {topN}')
hIndex0 = cursor.fetchone()[0]
print('MIN hIndex:', hIndex0)
filterCondition = f'hIndex_{suffix} >= {hIndex0}'

top_authors_path = f'out/{database}/top_authors.csv'
if os.path.exists(top_authors_path):
    print('top_authors.csv exists')
    top_authors_df = pd.read_csv(top_authors_path)
else:
    print('top_authors.csv not exists')
    top_authors_df = pd.read_sql(f"""select * from authors_{suffix}
        where {filterCondition}""", conn)
    top_authors_df.to_csv(top_authors_path, index=False)

top_authors_df['authorID'] = top_authors_df['authorID'].astype(str)
authorID_list = top_authors_df['authorID'].tolist()


print('loading data from database', datetime.datetime.now().strftime("%H:%M:%S"))
path_to_mapping = f"out/{database}/csv"
if not os.path.exists(path_to_mapping):
    os.makedirs(path_to_mapping)
    df_paper_author = pd.read_sql_query(f"select * from paper_author_{suffix}", conn)
    df_paper_author.to_csv(f"{path_to_mapping}/paper_author.csv", index=False)
    
    df_papers = pd.read_sql_query(f"select * from papers_{suffix}", conn)
    df_papers.to_csv(f"{path_to_mapping}/papers.csv", index=False)

    df_authors = pd.read_sql_query(f"select * from authors_{suffix}", conn)
    df_authors.to_csv(f"{path_to_mapping}/authors.csv", index=False)

    df_paper_reference = pd.read_sql_query(f"select * from paper_reference_{suffix}", conn)
    df_paper_reference.to_csv(f"{path_to_mapping}/paper_reference.csv", index=False)
else:
    df_paper_author = pd.read_csv(f"{path_to_mapping}/paper_author.csv")
    df_papers = pd.read_csv(f"{path_to_mapping}/papers.csv")
    df_authors = pd.read_csv(f"{path_to_mapping}/authors.csv")
    
    df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
    df_paper_author['paperID'] = df_paper_author['paperID'].astype(str)
    df_papers['paperID'] = df_papers['paperID'].astype(str)
    df_authors['authorID'] = df_authors['authorID'].astype(str)
    
df_paper_author_filtered = df_paper_author[df_paper_author['authorID'].isin(authorID_list)]
df_paper_author_filtered = df_paper_author[['authorID', 'paperID', 'authorOrder']].drop_duplicates()

