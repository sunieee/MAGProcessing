#此代码的功能是导出该field的papers表，paper_author表，reference表到csv，速度较慢

import pandas as pd
import numpy as np
import sys
from sqlalchemy import create_engine
from tqdm import tqdm
import os
import time
import sqlalchemy
import concurrent.futures

from utils import *

engine_MAG = create_engine('mysql+pymysql://root:root@192.168.0.140:3306/MACG', pool_size=20)
GROUP_SIZE = 100

####################################################################################
# extract paperID
# 根据领域名称查询fieldID，如： select * from field_of_study where name='Database';
####################################################################################
sql_data = '''select paperID from papers_field where fieldID = '''+'\''+field_+'\';'
db_data = pd.read_sql_query(sql_data, engine_MAG)
MAG=db_data['paperID'].values
print('finish reading MAG list from sql, saving to txt', len(MAG))

# save list(MAG) to a txt file
np.savetxt('out/MAG_'+field_+'.txt', MAG, fmt='%s')
print('finish saving MAG list to txt file')

# read MAG paperID list from txt file: MAG_field.txt
MAG = list(np.loadtxt('out/MAG_'+field_+'.txt', dtype=str))
print('finish reading MAG list from txt file', len(MAG))


####################################################################################
# get_data_from_table
# 从mysql中获取papers, paer_auther, paper_reference, authors四个表的field子数据，并保存到本地文件
####################################################################################

def get_data_from_table(table_name, key='paperID', data=MAG):
    t = time.time()
    db = pd.DataFrame()
    for i in tqdm(range(0, len(data), GROUP_SIZE)):
        # get a group of MAG paperID
        MAG_group = data[i:i+GROUP_SIZE]

        sql=f'''select * from {table_name} where '''\
              + key + ' in ('+','.join([f'\'{x}\'' for x in MAG_group])+')'

        db_temp = pd.read_sql_query(sql, engine_MAG)
        db=pd.concat([db,db_temp])

    print(f'{table_name}({key}) original', db.shape)
    db=db.drop_duplicates()
    print(f'{table_name}({key}) drop_duplicates', db.shape)

    print(f'{table_name}({key}) time cost: {time.time()-t}')
    db.name = table_name
    return db

def get_data_from_table_concurrent(table_name, key='paperID', data=MAG):
    print(f'Getting {table_name}({key}) from MAG')
    t = time.time()
    db = pd.DataFrame()
    query_params = [data[i:i+GROUP_SIZE] for i in range(0, len(data), GROUP_SIZE)]

    def _query(MAG_group):
        sql=f'''select * from {table_name} where '''\
              + key + ' in ('+','.join([f'\'{x}\'' for x in MAG_group])+')'
        # print(f"Executing query for param {index+1}/{len(query_params)}")
        return pd.read_sql_query(sql, engine_MAG)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        results = tqdm(executor.map(_query, query_params), total=len(query_params))

    # 将所有结果合并
    for result in results:
        db=pd.concat([db,result])

    print(f'{table_name}({key}) original', db.shape)
    db=db.drop_duplicates()
    print(f'{table_name}({key}) drop_duplicates', db.shape)

    print(f'{table_name}({key}) time cost: {time.time()-t}')
    return db


try:
    get_data_from_table_concurrent('papers').to_csv(f'out/papers_{field_}.csv',index=False)
except KeyboardInterrupt:
    pass

try:
    get_data_from_table_concurrent('paper_author').to_csv(f'out/paper_author_{field_}.csv',index=False)
except KeyboardInterrupt:
    pass

try:
    citing_db = get_data_from_table_concurrent('paper_reference', key='citingpaperID')
except KeyboardInterrupt:
    pass

try:
    cited_db = get_data_from_table_concurrent('paper_reference', key='citedpaperID')
    db = pd.concat([citing_db, cited_db])
    print('paper_reference original', db.shape)
    db=db.drop_duplicates()
    print('paper_reference drop_duplicates', db.shape)
    db.to_csv(f'out/paper_reference_{field_}.csv',index=False)
except KeyboardInterrupt:
    pass

paper_author_MAG = pd.read_csv(f'out/paper_author_{field_}.csv')
authors=paper_author_MAG['authorID'].drop_duplicates().values
get_data_from_table_concurrent('authors', key='authorID', data=authors).to_csv(f'out/authors_{field_}.csv',index=False)



####################################################################################
# to_sql
# 读取四个子表，并上传到mysql。创建表后添加领域子表的mysql索引（例如在scigene_database_field库）
####################################################################################
df_papers_MAG = pd.read_csv(f'out/papers_{field_}.csv')
print(df_papers_MAG)
print(df_papers_MAG.shape)
df_papers_MAG=df_papers_MAG.drop_duplicates()
print(df_papers_MAG.shape)
df_papers_MAG.to_sql('papers_field',con=engine,if_exists='replace',index=False, dtype={"paperID": sqlalchemy.types.NVARCHAR(length=100),\
    "title": sqlalchemy.types.NVARCHAR(length=2000),"ConferenceID": sqlalchemy.types.NVARCHAR(length=15),"JournalID": sqlalchemy.types.NVARCHAR(length=15),\
        "rank":sqlalchemy.types.INTEGER(),"referenceCount":sqlalchemy.types.INTEGER(),"citationCount":sqlalchemy.types.INTEGER(),"PublicationDate":sqlalchemy.types.Date()})


paper_author_MAG = pd.read_csv(f'out/paper_author_{field_}.csv')
print(paper_author_MAG)
print(paper_author_MAG.shape)
paper_author_MAG=paper_author_MAG.drop_duplicates()
print(paper_author_MAG.shape)
paper_author_MAG.to_sql('paper_author_field',con=engine,if_exists='replace',index=False, dtype={"paperID": sqlalchemy.types.NVARCHAR(length=15),\
    "authorID": sqlalchemy.types.NVARCHAR(length=15),"authorOrder":sqlalchemy.types.INTEGER()})


df_paper_reference_MAG = pd.read_csv(f'out/paper_reference_{field_}.csv')
print(df_paper_reference_MAG)
print(df_paper_reference_MAG.shape)
df_paper_reference_MAG=df_paper_reference_MAG.drop_duplicates()
print(df_paper_reference_MAG.shape)
df_paper_reference_MAG.to_sql('paper_reference_field',con=engine,if_exists='replace',index=False, dtype={"citingpaperID": sqlalchemy.types.NVARCHAR(length=15),\
    "citedpaperID": sqlalchemy.types.NVARCHAR(length=15)})


authors_MAG = pd.read_csv(f'out/authors_{field_}.csv')
print(authors_MAG)
print(authors_MAG.shape)
authors_MAG=authors_MAG.drop_duplicates()
print(authors_MAG.shape)
authors_MAG.to_sql('authors_field',con=engine,if_exists='replace',index=False, dtype={"authorID": sqlalchemy.types.NVARCHAR(length=15),\
    "name": sqlalchemy.types.NVARCHAR(length=999),"rank":sqlalchemy.types.INTEGER(),"PaperCount":sqlalchemy.types.INTEGER(),"CitationCount":sqlalchemy.types.INTEGER()})


# add index
execute('''ALTER TABLE papers_field ADD CONSTRAINT papers_field_pk PRIMARY KEY (paperID);
alter table papers_field add index(citationCount);
alter table paper_author_field add index(paperID);
alter table paper_author_field add index(authorID);
alter table paper_author_field add index(authorOrder);
alter table authors_field add index(authorID);
alter table authors_field add index(name);
alter table paper_reference_field add index(citingpaperID);
alter table paper_reference_field add index(citedpaperID);
ALTER TABLE paper_reference_field ADD CONSTRAINT paper_reference_field_pk PRIMARY KEY (citingpaperID,citedpaperID);
''')
       
'''
alter table papers_field ADD abstract mediumtext;
update papers_field as P, MACG.abstracts as abs set P.abstract = abs.abstract where P.paperID = abs.paperID

-- delete abstract mediumtext
ALTER TABLE papers_field DROP abstract;
'''