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
import multiprocessing

from utils import *

engine_MAG = create_engine('mysql+pymysql://root:root@192.168.0.140:3306/MACG', pool_size=20)
GROUP_SIZE = 2000
multiproces_num = 20

####################################################################################
# extract paperID
# 根据领域名称查询fieldID，如： select * from field_of_study where name='Database';
####################################################################################



def get_paperID_batch(pair):
    fieldID, offset, verbose = pair
    engine = create_engine('mysql+pymysql://root:root@192.168.0.140:3306/MACG')
    sql_data = f'select paperID from papers_field where fieldID=\'{fieldID}\' LIMIT {GROUP_SIZE} OFFSET {offset};'
    if verbose:
        print('* ' + sql_data)
    db_data_single = pd.read_sql_query(sql_data, engine)['paperID'].tolist()
    engine.dispose()
    return db_data_single


def worker(task_queue, result_queue):
    while True:
        task = task_queue.get()
        # 这里，你可以运行你的函数，如 get_paperID_batch
        result = get_paperID_batch(task)
        if len(result):
            result_queue.put(result)
        else:
            break


def read_papers(fields, verbose=True):
    paper_ids = set()
    for fieldID in tqdm(set(fields)):
        # 使用分页查询（也叫做分块查询）来获取所有的paperID
        offset = 0
        finish = False
        db_data_single = set()
        while not finish:
            with multiprocessing.Pool(processes=multiproces_num) as pool:
                results = pool.map(get_paperID_batch, [(fieldID, offset+i*GROUP_SIZE, verbose) for i in range(multiproces_num)])
                for result in results:
                    db_data_single.update(result)
                    if len(result) == 0:
                        finish = True
            offset += multiproces_num * GROUP_SIZE
        paper_ids.update(db_data_single)
        if verbose:
            print(f'finish reading paperID on field {fieldID}, single: {len(db_data_single)}, all: {len(paper_ids)}')
    return paper_ids

db_data = read_papers(field_info['fieldID'])
print(f'## finish reading paperID on fieldID:', len(db_data))

for fieldID in set(field_info.get('children', [])):
    sql_data = f'select childrenID FROM field_children where parentID=\'{fieldID}\';'
    children_fields = pd.read_sql_query(sql_data, engine_MAG).values.ravel().tolist()
    print('*', sql_data, len(children_fields))
    db_data_single = read_papers(children_fields, verbose=False)
    db_data.update(db_data_single)
    print(f'finish reading paperID on children of {fieldID}, single: {len(db_data_single)}, all: {len(db_data)}')
print(f'## finish reading paperID on children:', len(db_data))

for journalID in tqdm(set(field_info.get('JournalID', []))):
    sql_data = f'select paperID from papers where JournalID=\'{journalID}\';'
    print('*', sql_data)
    db_data_single = pd.read_sql_query(sql_data, engine_MAG)['paperID'].tolist()
    db_data.update(db_data_single)
    print(f'finish reading paperID on Journal {journalID}, single: {len(db_data_single)}, all: {len(db_data)}')
print(f'## finish reading paperID on Journal:', len(db_data))

for conferenceID in tqdm(set(field_info.get('ConferenceID', []))):
    sql_data = f'select paperID from papers where ConferenceID=\'{conferenceID}\';'
    print('*', sql_data)
    db_data_single = pd.read_sql_query(sql_data, engine_MAG)['paperID'].tolist()
    db_data.update(db_data_single)
    print(f'finish reading paperID on Conference {conferenceID}, single: {len(db_data_single)}, all: {len(db_data)}')
print(f'## finish reading paperID on Conference:', len(db_data))

papers = list(db_data)
print('# finish reading MAG list from sql, saving to txt', len(papers))

# save list(MAG) to a txt file
np.savetxt(f'out/{database}/papers.txt', papers, fmt='%s')
print('# finish saving MAG list to txt file')

# read MAG paperID list from txt file: MAG_field.txt
papers = list(np.loadtxt(f'out/{database}/papers.txt', dtype=str))
print('# finish reading MAG list from txt file', len(papers))


####################################################################################
# get_data_from_table
# 从mysql中获取papers, paer_auther, paper_reference, authors四个表的field子数据，并保存到本地文件
####################################################################################

def get_data_from_table(table_name, key='paperID', data=papers):
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

def get_data_from_table_concurrent(table_name, key='paperID', data=papers):
    print(f'# Getting {table_name}({key}) from MAG')
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
    get_data_from_table_concurrent('papers').to_csv(f'out/{database}/papers.csv',index=False)
except KeyboardInterrupt:
    pass

try:
    get_data_from_table_concurrent('paper_author').to_csv(f'out/{database}/paper_author.csv',index=False)
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
    db.to_csv(f'out/{database}/paper_reference.csv',index=False)
except KeyboardInterrupt:
    pass

paper_author_MAG = pd.read_csv(f'out/{database}/paper_author.csv')
authors=paper_author_MAG['authorID'].drop_duplicates().values
get_data_from_table_concurrent('authors', key='authorID', data=authors).to_csv(f'out/{database}/authors.csv',index=False)



####################################################################################
# to_sql
# 读取四个子表，并上传到mysql。创建表后添加领域子表的mysql索引（例如在scigene_database_field库）
####################################################################################
df_papers_MAG = pd.read_csv(f'out/{database}/papers.csv')
print(df_papers_MAG)
print(df_papers_MAG.shape)
df_papers_MAG=df_papers_MAG.drop_duplicates()
print(df_papers_MAG.shape)
df_papers_MAG.to_sql('papers_field',con=engine,if_exists='replace',index=False, dtype={"paperID": sqlalchemy.types.NVARCHAR(length=100),\
    "title": sqlalchemy.types.NVARCHAR(length=2000),"ConferenceID": sqlalchemy.types.NVARCHAR(length=15),"JournalID": sqlalchemy.types.NVARCHAR(length=15),\
        "rank":sqlalchemy.types.INTEGER(),"referenceCount":sqlalchemy.types.INTEGER(),"citationCount":sqlalchemy.types.INTEGER(),"PublicationDate":sqlalchemy.types.Date()})


paper_author_MAG = pd.read_csv(f'out/{database}/paper_author.csv')
print(paper_author_MAG)
print(paper_author_MAG.shape)
paper_author_MAG=paper_author_MAG.drop_duplicates()
print(paper_author_MAG.shape)
paper_author_MAG.to_sql('paper_author_field',con=engine,if_exists='replace',index=False, dtype={"paperID": sqlalchemy.types.NVARCHAR(length=15),\
    "authorID": sqlalchemy.types.NVARCHAR(length=15),"authorOrder":sqlalchemy.types.INTEGER()})


df_paper_reference_MAG = pd.read_csv(f'out/{database}/paper_reference.csv')
print(df_paper_reference_MAG)
print(df_paper_reference_MAG.shape)
df_paper_reference_MAG=df_paper_reference_MAG.drop_duplicates()
print(df_paper_reference_MAG.shape)
df_paper_reference_MAG.to_sql('paper_reference_field',con=engine,if_exists='replace',index=False, dtype={"citingpaperID": sqlalchemy.types.NVARCHAR(length=15),\
    "citedpaperID": sqlalchemy.types.NVARCHAR(length=15)})


authors_MAG = pd.read_csv(f'out/{database}/authors.csv')
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

alter table papers_field ADD abstract mediumtext;
update papers_field as P, MACG.abstracts as abs set P.abstract = abs.abstract where P.paperID = abs.paperID        
''')
       
'''
alter table papers_field ADD abstract mediumtext;
update papers_field as P, MACG.abstracts as abs set P.abstract = abs.abstract where P.paperID = abs.paperID

-- delete abstract mediumtext
ALTER TABLE papers_field DROP abstract;
'''