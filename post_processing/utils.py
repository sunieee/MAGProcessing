import os
import pymysql
import time
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd

fieldName = os.environ.get('fieldName', 'visualization')

# 对于 authorID 的限制
authorID_list = ['2147343253', '2076420186', '2122885999', 
                 '2003408012', '2762167099', '2158935544', '3206897746']
# authorID_list = ['3206897746']
# 3206897746    2147343253
ids_string = ', '.join(map(str, authorID_list))
filterCondition = f"authorID IN ({ids_string})"
print(filterCondition)

edge_df = pd.read_csv(f'data/{fieldName}/edge_proba.csv')
edge_df['citingpaperID'] = edge_df['citingpaperID'].astype(str)
edge_df['citedpaperID'] = edge_df['citedpaperID'].astype(str)
edge_df['authorID'] = edge_df['authorID'].astype(str)

node_dir = f'data/{fieldName}'


database = f"scigene_{fieldName}_field"
engine = create_engine(f"mysql+pymysql://root:root@localhost:3306/{database}?charset=utf8")
conn = pymysql.connect(host='localhost',
                            port=3306,
                            user='root',
                            password='root',
                            db=database,
                            charset='utf8')
cursor = conn.cursor()

conn118 = pymysql.connect(host='192.168.0.118',
                            port=3306,
                            user='root',
                            password='Vis_2014',
                            db='MACG',
                            charset='utf8')
cursor118 = conn118.cursor()


def execute(sql):
    for _sql in sql.split(';'):
        _sql = _sql.strip()
        if _sql == '':
            continue
        print('executing: ', _sql)
        t = time.time()
        cursor.execute(_sql)
        conn.commit()
        print('[time cost: ', time.time()-t, ']')

def executeFetch(sql, cursor=cursor):
    sql = sql.strip()
    t = time.time()
    print('executing: ', sql)
    cursor.execute(sql)
    rows = cursor.fetchall()
    print('[time cost: ', time.time()-t, ']')
    return rows


