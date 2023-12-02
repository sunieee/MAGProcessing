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
import datetime
from utils import database, execute, cursor, conn, engine, field_info

userpass = f'{os.environ.get("user")}:{os.environ.get("password")}'
engine_MAG = create_engine(f'mysql+pymysql://{userpass}@192.168.0.140:3306/MACG', pool_size=20)
GROUP_SIZE = 2000
multiproces_num = 20

####################################################################################
# extract paperID
# 根据领域名称查询fieldID，如： select * from field_of_study where name='Database';
####################################################################################

def get_paperID_batch(pair):
    fieldID, offset, ix, pbar, verbose = pair
    engine = create_engine(f'mysql+pymysql://{userpass}@192.168.0.140:3306/MACG')
    sql_data = f'select paperID from papers_field where fieldID=\'{fieldID}\' LIMIT {GROUP_SIZE} OFFSET {offset};'
    # if verbose:
    #     time = datetime.datetime.now().strftime('%H:%M:%S')
    #     print(f'* {time} ' + sql_data)
    db_data_single = pd.read_sql_query(sql_data, engine)['paperID'].tolist()
    engine.dispose()
    if verbose:
        pbar.n = int(ix)
        pbar.refresh()
    return db_data_single


paper_count_df = pd.DataFrame(columns=['type', 'ID', 'paperCount', 'accumulateCount'])
def read_papers(fields, verbose=True):
    paper_ids = set()
    for fieldID in tqdm(fields):
        try:
            fieldID = int(fieldID)
            cursor.execute(f"SELECT paperCount FROM MACG.field_of_study where fieldID='{fieldID}'")
            result = cursor.fetchone()
            paperCount = result[0]
        except:
            fieldName = fieldID
            cursor.execute(f"SELECT fieldID, paperCount FROM MACG.field_of_study where name='{fieldName}'")
            result = cursor.fetchone()
            fieldID = result[0]
            paperCount = result[1]

        group_num = paperCount // GROUP_SIZE + 5
        pbar = tqdm(total=group_num)
        print(f'filedID: {fieldID}, paperCount: {paperCount}, group_num: {group_num}')
        with concurrent.futures.ThreadPoolExecutor(max_workers=multiproces_num) as executor:
            results = executor.map(get_paperID_batch, [(fieldID, i*GROUP_SIZE, i, pbar, verbose) for i in range(group_num)])
        
        # 将所有结果合并
        db_data_single = set()
        for result in tqdm(results):
            db_data_single.update(result)

        paper_ids.update(db_data_single)
        paper_count_df.loc[len(paper_count_df)] = ['field', fieldID, len(db_data_single), len(paper_ids)]
        if verbose:
            print(f'finish reading paperID on field {fieldID}, single: {len(db_data_single)}, all: {len(paper_ids)}')
    return paper_ids

paper_path = f'out/{database}/papers.txt'
if os.path.exists(paper_path):
    # read MAG paperID list from txt file: MAG_field.txt
    papers = list(np.loadtxt(paper_path, dtype=str))
    print(f'# {paper_path} exists, reading MAG list from txt file', len(papers))
else:
    db_data = read_papers(field_info['fieldID'])
    print(f'## finish reading paperID on fieldID:', len(db_data))

    for fieldID in field_info.get('children', []):
        sql_data = f'select childrenID FROM field_children where parentID=\'{fieldID}\';'
        children_fields = pd.read_sql_query(sql_data, engine_MAG).values.ravel().tolist()
        print('*', sql_data, len(children_fields))
        db_data_single = read_papers(children_fields, verbose=False)
        db_data.update(db_data_single)
        print(f'finish reading paperID on children of {fieldID}, single: {len(db_data_single)}, all: {len(db_data)}')
    print(f'## finish reading paperID on children:', len(db_data))

    for journalID in tqdm(field_info.get('JournalID', [])):
        sql_data = f'select paperID from papers where JournalID=\'{journalID}\';'
        print('*', sql_data)
        db_data_single = pd.read_sql_query(sql_data, engine_MAG)['paperID'].tolist()
        db_data.update(db_data_single)
        paper_count_df.loc[len(paper_count_df)] = ['journal', journalID, len(db_data_single), len(db_data)]
        print(f'finish reading paperID on Journal {journalID}, single: {len(db_data_single)}, all: {len(db_data)}')
    print(f'## finish reading paperID on Journal:', len(db_data))

    for conferenceID in tqdm(field_info.get('ConferenceID', [])):
        sql_data = f'select paperID from papers where ConferenceID=\'{conferenceID}\';'
        print('*', sql_data)
        db_data_single = pd.read_sql_query(sql_data, engine_MAG)['paperID'].tolist()
        db_data.update(db_data_single)
        paper_count_df.loc[len(paper_count_df)] = ['conference', conferenceID, len(db_data_single), len(db_data)]
        print(f'finish reading paperID on Conference {conferenceID}, single: {len(db_data_single)}, all: {len(db_data)}')
    print(f'## finish reading paperID on Conference:', len(db_data))

    papers = list(db_data)
    print('# finish reading MAG list from sql, saving to txt', len(papers))

    # save list(MAG) to a txt file
    np.savetxt(f'out/{database}/papers.txt', papers, fmt='%s')
    print('# finish saving MAG list to txt file')

    paper_count_df.to_csv(f'out/{database}/paper_count.csv', index=False)


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

    def _query(pair):
        MAG_group, index, pbar = pair
        engine = create_engine(f'mysql+pymysql://{userpass}@192.168.0.140:3306/MACG')
        sql=f'''select * from {table_name} where '''\
              + key + ' in ('+','.join([f'\'{x}\'' for x in MAG_group])+')'
        # time = datetime.datetime.now().strftime('%H:%M:%S')
        # print(f"* {time} Executing query for param {index+1}/{len(query_params)} in {table_name}({key})")
        ret = pd.read_sql_query(sql, engine)
        engine.dispose()

        pbar.n = int(index)
        pbar.refresh()
        return ret
    
    group_num = len(range(0, len(data), GROUP_SIZE))
    pbar = tqdm(total=group_num)
    params = [(data[i*GROUP_SIZE:(i+1)*GROUP_SIZE], i, pbar) for i in range(group_num)]
    print(f'## create params in {table_name}({key}), length: {group_num}')
    with concurrent.futures.ThreadPoolExecutor(max_workers=multiproces_num * 5) as executor:
        results = executor.map(_query, params)

    # 将所有结果合并
    # for i in tqdm(range(len(results))):
    #     db=pd.concat([db, results[i]])
    st = time.time()
    db = pd.concat(results)
    print(f'## finish reading {table_name}({key}), merge time cost: {time.time()-st}')

    print(f'{table_name}({key}) original', db.shape)
    db=db.drop_duplicates()
    print(f'{table_name}({key}) drop_duplicates', db.shape, f'time cost: {time.time()-t}')
    return db

print('# getting papers from MAG', datetime.datetime.now().strftime('%H:%M:%S'))
df_papers_MAG = get_data_from_table_concurrent('papers')
# df_papers_MAG.to_csv(f'out/{database}/papers.csv',index=False)

paper_author_MAG = get_data_from_table_concurrent('paper_author')
authors=paper_author_MAG['authorID'].drop_duplicates().values
# paper_author_MAG.to_csv(f'out/{database}/paper_author.csv',index=False)

citing_db = get_data_from_table_concurrent('paper_reference', key='citingpaperID')
cited_db = get_data_from_table_concurrent('paper_reference', key='citedpaperID')
df_paper_reference_MAG = pd.concat([citing_db, cited_db])
print('paper_reference original', df_paper_reference_MAG.shape)
df_paper_reference_MAG=df_paper_reference_MAG.drop_duplicates()
print('paper_reference drop_duplicates', df_paper_reference_MAG.shape)
# df_paper_reference_MAG.to_csv(f'out/{database}/paper_reference.csv',index=False)

authors_MAG = get_data_from_table_concurrent('authors', key='authorID', data=authors)
# authors_MAG.to_csv(f'out/{database}/authors.csv',index=False)


####################################################################################
# to_sql
# 读取四个子表，并上传到mysql。创建表后添加领域子表的mysql索引（例如在scigene_database_field库）
####################################################################################
print('## uploading papers', datetime.datetime.now().strftime('%H:%M:%S'))
# df_papers_MAG = pd.read_csv(f'out/{database}/papers.csv')
print(df_papers_MAG, df_papers_MAG.shape)
df_papers_MAG.to_sql('papers_field',con=engine,if_exists='replace',index=False, dtype={"paperID": sqlalchemy.types.NVARCHAR(length=100),\
    "title": sqlalchemy.types.NVARCHAR(length=2000),"ConferenceID": sqlalchemy.types.NVARCHAR(length=15),"JournalID": sqlalchemy.types.NVARCHAR(length=15),\
        "rank":sqlalchemy.types.INTEGER(),"referenceCount":sqlalchemy.types.INTEGER(),"citationCount":sqlalchemy.types.INTEGER(),"PublicationDate":sqlalchemy.types.Date()})

print('## uploading paper_author', datetime.datetime.now().strftime('%H:%M:%S'))
# paper_author_MAG = pd.read_csv(f'out/{database}/paper_author.csv')
print(paper_author_MAG, paper_author_MAG.shape)
paper_author_MAG.to_sql('paper_author_field',con=engine,if_exists='replace',index=False, dtype={"paperID": sqlalchemy.types.NVARCHAR(length=15),\
    "authorID": sqlalchemy.types.NVARCHAR(length=15),"authorOrder":sqlalchemy.types.INTEGER()})

print('## uploading paper_reference', datetime.datetime.now().strftime('%H:%M:%S'))
# df_paper_reference_MAG = pd.read_csv(f'out/{database}/paper_reference.csv')
print(df_paper_reference_MAG, df_paper_reference_MAG.shape)
df_paper_reference_MAG.to_sql('paper_reference_field',con=engine,if_exists='replace',index=False, dtype={"citingpaperID": sqlalchemy.types.NVARCHAR(length=15),\
    "citedpaperID": sqlalchemy.types.NVARCHAR(length=15)})

print('## uploading authors', datetime.datetime.now().strftime('%H:%M:%S'))
# authors_MAG = pd.read_csv(f'out/{database}/authors.csv')
print(authors_MAG, authors_MAG.shape)
authors_MAG.to_sql('authors_field',con=engine,if_exists='replace',index=False, dtype={"authorID": sqlalchemy.types.NVARCHAR(length=15),\
    "name": sqlalchemy.types.NVARCHAR(length=999),"rank":sqlalchemy.types.INTEGER(),"PaperCount":sqlalchemy.types.INTEGER(),"CitationCount":sqlalchemy.types.INTEGER()})

# add index
print('## add index', datetime.datetime.now().strftime('%H:%M:%S'))
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
       
# 直接update abstract太慢了，后续使用多进程下载
'''
alter table papers_field ADD abstract mediumtext;
update papers_field as P, MACG.abstracts as abs set P.abstract = abs.abstract where P.paperID = abs.paperID

-- delete abstract mediumtext
ALTER TABLE papers_field DROP abstract;
'''

cursor.close()
conn.close()