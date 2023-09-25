import pandas as pd
import numpy as np
from sqlalchemy import create_engine

import sys
if len(sys.argv) < 2:
    print("Not enough parameters: ", len(sys.argv))
    sys.exit

database = sys.argv[1]

engine = create_engine('mysql+pymysql://root:Vis_2014@localhost:3306/'+database)


sql_paper = '''select * from all_dataset_link; '''
db_data=pd.read_sql_query(sql_paper, engine)

MAG1=db_data['citingpaperID']
MAG2=db_data['citedpaperID']
MAG=pd.concat([MAG1,MAG2])
MAG=MAG.drop_duplicates().values
if database=="scigene_acl_anthology":
    sql2 = '''select * from papers_ARC;'''
else:
    sql2 = '''select * from papers_field;'''

db = pd.read_sql_query(sql2, engine)

db = db[['paperID','title','abstract']]
db = db[db.paperID.isin(MAG)]
print('similarity_mysql:',db)
db.to_csv('similarity_mysql.csv',index=False)