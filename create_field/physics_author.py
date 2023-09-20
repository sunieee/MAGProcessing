import pandas as pd
import numpy as np
from sqlalchemy import create_engine
engine_MAG = create_engine('mysql+pymysql://root:Vis_2014@localhost:3306/MACG')
from tqdm import tqdm

paper_author_MAG = pd.read_csv('analysis_physics_author.csv')
print(paper_author_MAG)
paper_author = paper_author_MAG.groupby('authorID')
df = paper_author.filter(lambda x: len(x) >= 100).groupby('authorID').size()
true_author = pd.DataFrame(df).index.values
print(len(true_author))
paper_author_MAG = paper_author_MAG[['paperID','authorID','authorOrder']]
paper_author_MAG = paper_author_MAG[paper_author_MAG.authorID.isin(true_author)].drop_duplicates()
print(paper_author_MAG)
paper_author_MAG.to_csv('paper_author_MAG.csv',index=False)

author=paper_author_MAG['authorID'].drop_duplicates().values

flag=0
sql=''
for i in range(author.shape[0]):
    sql=sql+'authorID=\''+str(author[i])+'\' or '
    if(i%100==0):
        sql1 = '''select * from authors where '''
        sql=sql1+sql
        sql=sql[:-4]+';'
        db_temp = pd.read_sql_query(sql, engine_MAG)
        if(flag==0):
            db=db_temp
            flag=1
        else:
            db=pd.concat([db,db_temp])
            print(db)
        sql=''
sql1 = '''select * from authors where '''
sql=sql1+sql
sql=sql[:-4]+';'
db_temp = pd.read_sql_query(sql, engine_MAG)
if(flag==0):
    db=db_temp
    flag=1
else:
    db=pd.concat([db,db_temp])
    print(db)

db.to_csv('authors_MAG.csv',index=False)

MAG = paper_author_MAG['paperID'].drop_duplicates().values
print(len(MAG))

flag=0
sql=''
for i in range(MAG.shape[0]):
    sql=sql+'paperID=\''+str(MAG[i])+'\' or '
    if(i%100==0):
        sql1 = '''select * from papers where '''
        sql=sql1+sql
        sql=sql[:-4]+';'
        db_temp = pd.read_sql_query(sql, engine_MAG)
        if(flag==0):
            db=db_temp
            flag=1
        else:
            db=pd.concat([db,db_temp])
            print(db.shape)
        sql=''
sql1 = '''select * from papers where '''
sql=sql1+sql
sql=sql[:-4]+';'
db_temp = pd.read_sql_query(sql, engine_MAG)
if(flag==0):
    db=db_temp
    flag=1
else:
    db=pd.concat([db,db_temp])
    print(db.shape)

db.to_csv('papers_MAG.csv',index=False)


flag=0
sql=''
for i in range(MAG.shape[0]):
    sql=sql+'citingpaperID=\''+str(MAG[i])+'\' or '+'citedpaperID=\''+str(MAG[i])+'\' or '
    if(i%100==0):
        sql1 = '''select * from paper_reference where '''
        sql=sql1+sql
        sql=sql[:-4]+';'
        try:
            db_temp = pd.read_sql_query(sql, engine_MAG)
        except:
            sql=''
            continue
        if(flag==0):
            db=db_temp
            flag=1
        else:
            db=pd.concat([db,db_temp])
            print(db.shape,i)
        sql=''
sql1 = '''select * from paper_reference where '''
sql=sql1+sql
sql=sql[:-4]+';'
db_temp = pd.read_sql_query(sql, engine_MAG)
if(flag==0):
    db=db_temp
    flag=1
else:
    db=pd.concat([db,db_temp])
    print(db.shape)
    db=db.drop_duplicates()
    print(db.shape)

db.to_csv('paper_reference_MAG.csv',index=False)