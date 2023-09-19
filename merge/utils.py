from sqlalchemy import create_engine
import pymysql
import time

import sys

if len(sys.argv) < 3:
    print("Not enough parameters: ", len(sys.argv))
    sys.exit

field_ = sys.argv[1]
database = sys.argv[2]

# database_mapping = {
#     '77088390': 'scigene_database_field',
#     '121332964': 'scigene_physics_field'
# }
# database = database_mapping[field_]

engine = create_engine('mysql+pymysql://root:root@192.168.0.140:3306/'+database)
connection = pymysql.connect(host='localhost',
                            port=3306,
                            user='root',
                            password='root',
                            db=database,
                            charset='utf8')
cursor = connection.cursor()

def execute(sql):
    for _sql in sql.split(';'):
        _sql = _sql.strip()
        if _sql == '':
            continue
        print('executing: ', _sql)
        t = time.time()
        cursor.execute(_sql)
        connection.commit()
        print('time cost: ', time.time()-t)
