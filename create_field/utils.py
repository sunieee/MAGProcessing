from sqlalchemy import create_engine
import pymysql
import time
import json
import numpy as np

import sys
import os


database = os.environ.get('database', 'scigene_database_field')

# read config.yaml
import yaml
with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
field_info = config[database]

# database_mapping = {
#     '77088390': 'scigene_database_field',
#     '121332964': 'scigene_physics_field'
# }
# database = database_mapping[field_]

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
engine = create_engine('mysql+pymysql://root:root@192.168.0.140:3306/'+database)


def execute(sql):
    for _sql in sql.split(';'):
        _sql = _sql.strip()
        if _sql == '':
            continue
        print('*', _sql)
        t = time.time()
        cursor.execute(_sql)
        conn.commit()
        print('* time:', time.time()-t)

def try_execute(sql):
    try:
        cursor.execute(sql)
    except:
        pass
    conn.commit()

def executeFetch(sql):
    sql = sql.strip()
    t = time.time()
    cursor.execute(sql)
    rows = cursor.fetchall()
    print('[time cost: ', time.time()-t, ']')
    return rows

class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for np types """

    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32,
                              np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        elif isinstance(obj, (np.bool_,)):
            return bool(obj)
        return json.JSONEncoder.default(self, obj)