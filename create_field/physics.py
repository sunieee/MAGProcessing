import pandas as pd
import numpy as np
from sqlalchemy import create_engine
engine_MAG = create_engine('mysql+pymysql://root:Vis_2014@localhost:3306/MACG')
from tqdm import tqdm
# sql_data = '''SELECT * FROM authors ORDER BY CitationCount DESC limit 20000;'''
# db_data = pd.read_sql_query(sql_data, engine_MAG)
# db_data.to_csv('physics_author.csv',index=False)

paper_author_MAG = pd.read_csv('physics_author.csv')
author=paper_author_MAG['authorID'].drop_duplicates().values

flag=0
sql=''
count = 0 
for i in tqdm(range(author.shape[0])):
    sql=sql+'paper_author.authorID=\''+str(author[i])+'\' or '
    if(i%100==0):
        sql1 = '''select * from papers_field, paper_author where papers_field.fieldID='121332964' and papers_field.paperID = paper_author.paperID and (''' 
        sql=sql1+sql
        sql=sql[:-4]+');'
        print(sql)
        db_temp = pd.read_sql_query(sql, engine_MAG)
        if(flag==0):
            db=db_temp
            flag=1
        else:
            db=pd.concat([db,db_temp])
            print(db.shape,i)
        sql=''
        print(db)
        db.to_csv('analysis_physics_author.csv',index=False)