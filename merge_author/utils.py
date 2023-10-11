import os
import pymysql
import time
from sqlalchemy import create_engine
import pandas as pd
import json
import numpy


database = os.environ.get('database', 'scigene_visualization_field')

# 对于 authorID 的限制
# authorID_list = ['2147343253', '2076420186', '2122885999', 
#                  '2003408012', '2762167099', '2158935544']
# authorID_list = ['3206897746']
# ids_string = ', '.join(map(str, authorID_list))

# filterCondition = f"authorID IN ({ids_string})"


conn = pymysql.connect(host='localhost',
                            port=3306,
                            user='root',
                            password='root',
                            db=database,
                            charset='utf8')
cursor = conn.cursor()
engine = create_engine('mysql+pymysql://root:root@192.168.0.140:3306/'+database)


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

class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """

    def default(self, obj):
        if isinstance(obj, (numpy.int_, numpy.intc, numpy.intp, numpy.int8,
                            numpy.int16, numpy.int32, numpy.int64, numpy.uint8,
                            numpy.uint16, numpy.uint32, numpy.uint64)):
            return int(obj)
        elif isinstance(obj, (numpy.float_, numpy.float16, numpy.float32,
                              numpy.float64)):
            return float(obj)
        elif isinstance(obj, (numpy.ndarray,)):
            return obj.tolist()
        elif isinstance(obj, (numpy.bool_,)):
            return bool(obj)
        return json.JSONEncoder.default(self, obj)