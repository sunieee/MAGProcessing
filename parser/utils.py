import pymysql
import time
import sys
import os

fieldName = os.environ.get('fieldName', 'visualization')
numOfTopAuthors = os.environ.get('numOfTopAuthors', 1100)
numOfTopAuthors = int(numOfTopAuthors)

filterCondition = f"authorRank <= {numOfTopAuthors}"
filterCondition = "authorID = '3206897746'"

# authorRank > 1000 and authorRank <= {numOfTopAuthors};

database = f"scigene_{fieldName}_field_pcg"
conn = pymysql.connect(host='localhost',
                            port=3306,
                            user='root',
                            password='root',
                            db=database,
                            charset='utf8')
cursor = conn.cursor()

def execute(sql):
    for _sql in sql.split(';'):
        _sql = _sql.strip()
        if _sql == '':
            continue
        print('executing: ', _sql)
        t = time.time()
        cursor.execute(_sql)
        conn.commit()
        print('time cost: ', time.time()-t)