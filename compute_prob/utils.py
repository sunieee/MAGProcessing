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

def try_execute(sql, cursor=cursor):
    try:
        cursor.execute(sql)
    except:
        pass
    conn.commit()


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


print('loading data from database', datetime.datetime.now().strftime("%H:%M:%S"))
path_to_mapping = f"out/{database}/csv"
if not os.path.exists(path_to_mapping):
    os.makedirs(path_to_mapping)
    df_paper_author_field = pd.read_sql_query(f"select * from paper_author_field", conn)
    df_paper_author_field.to_csv(f"{path_to_mapping}/paper_author_field.csv", index=False)
    
    df_papers_field = pd.read_sql_query(f"select * from papers_field", conn)
    df_papers_field.to_csv(f"{path_to_mapping}/papers_field.csv", index=False)

    df_authors_field = pd.read_sql_query(f"select * from authors_field", conn)
    df_authors_field.to_csv(f"{path_to_mapping}/authors_field.csv", index=False)

    df_paper_reference_field = pd.read_sql_query(f"select * from paper_reference_field", conn)
    df_paper_reference_field.to_csv(f"{path_to_mapping}/paper_reference_field.csv", index=False)
else:
    df_paper_author_field = pd.read_csv(f"{path_to_mapping}/paper_author_field.csv")
    df_papers_field = pd.read_csv(f"{path_to_mapping}/papers_field.csv")
    df_authors_field = pd.read_csv(f"{path_to_mapping}/authors_field.csv")
    df_paper_reference_field = pd.read_csv(f"{path_to_mapping}/paper_reference_field.csv")

    df_paper_author_field['authorID'] = df_paper_author_field['authorID'].astype(str)
    df_paper_author_field['paperID'] = df_paper_author_field['paperID'].astype(str)
    df_papers_field['paperID'] = df_papers_field['paperID'].astype(str)
    df_authors_field['authorID'] = df_authors_field['authorID'].astype(str)
    df_paper_reference_field['citingpaperID'] = df_paper_reference_field['citingpaperID'].astype(str)
    df_paper_reference_field['citedpaperID'] = df_paper_reference_field['citedpaperID'].astype(str)


# 直接从paper_reference_field表中筛选出自引的记录
print('creating node & edges', datetime.datetime.now().strftime("%H:%M:%S"))
if not os.path.exists(f'out/{database}/edges.csv'):
    print('edges.csv not found, creating self-reference graph...')
    t = time.time()
    # 使用两次 merge 来模拟 SQL 中的 join 操作
    df_paper_author_field_filtered = df_paper_author_field[df_paper_author_field['authorID'].isin(authorID_list)]
    df_paper_author_field_filtered = df_paper_author_field_filtered[['paperID', 'authorID']].drop_duplicates()

    merged_df1 = df_paper_reference_field.merge(df_paper_author_field_filtered, left_on='citingpaperID', right_on='paperID')
    merged_df2 = merged_df1.merge(df_paper_author_field_filtered.rename(columns={'authorID': 'authorID2', 'paperID': 'paperID2'}), 
                                    left_on='citedpaperID', right_on='paperID2')
    edges = merged_df2[merged_df2['authorID'] == merged_df2['authorID2']]
    edges = edges[['authorID', 'citingpaperID', 'citedpaperID']]

    print(f'edges created, time cost:', time.time()-t)
    edges.to_csv(f'out/{database}/edges.csv', index=False)    
else:   
    edges = pd.read_csv(f'out/{database}/edges.csv')
    edges['authorID'] = edges['authorID'].astype(str)
    edges['citingpaperID'] = edges['citingpaperID'].astype(str)
    edges['citedpaperID'] = edges['citedpaperID'].astype(str)

edges_by_citing = edges.set_index('citingpaperID')
edges_by_cited = edges.set_index('citedpaperID')

nodes = pd.concat([edges['citingpaperID'], edges['citedpaperID']])
nodes = tuple(nodes.drop_duplicates().values)
print('nodes:', len(nodes), 'edges:', len(edges))



