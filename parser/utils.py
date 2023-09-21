import pymysql
from sqlalchemy import create_engine
import time
import sys
import os

fieldName = os.environ.get('fieldName', 'visualization')
numOfTopAuthors = os.environ.get('numOfTopAuthors', 1100)
numOfTopAuthors = int(numOfTopAuthors)

filterCondition = f"authorRank <= {numOfTopAuthors}"
filterCondition = "authorID = '2147343253'"

# authorRank > 1000 and authorRank <= {numOfTopAuthors};

database = f"scigene_{fieldName}_field_pcg"
engine = create_engine(f"mysql+pymysql://root:root@192.168.0.140:3306/{database}?charset=utf8")
conn = pymysql.connect(host='localhost',
                            port=3306,
                            user='root',
                            password='root',
                            db=database,
                            charset='utf8')
cursor = conn.cursor()

# 当你使用pymysql直接创建的连接，它返回的是一个原生的MySQL连接，
# 而pandas的to_sql方法期望一个SQLAlchemy引擎作为其连接参数。

# 为了使用pandas的to_sql方法与MySQL数据库，你需要使用SQLAlchemy来创建一个连接引擎。
# to_sql需要更多的功能，如检查表是否存在、创建表、插入数据等。
# 这些功能在不同的数据库中可能会有所不同，因此pandas依赖于SQLAlchemy来提供这种数据库无关的接口

# pandas的read_sql_query和read_sql函数设计得更为灵活，
# 它们可以接受原生的数据库连接（如通过pymysql创建的连接）或SQLAlchemy引擎作为其连接参数。
# 这是为了方便用户从各种数据库中读取数据。


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


def executeFetch(sql):
    sql = sql.strip()
    t = time.time()
    cursor.execute(sql)
    rows = cursor.fetchall()
    print('[time cost: ', time.time()-t, ']')
    return rows