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
import json
from utils import field, execute, cursor, conn, engine, field_info, try_execute
from datetime import datetime

userpass = f'{os.environ.get("user")}:{os.environ.get("password")}'
GROUP_SIZE = 2000
multiproces_num = 20

paper_path = f'out/{field}/csv/papers.txt'
papers = list(np.loadtxt(paper_path, dtype=str))

####################################################################################
# get_data_from_table
# 从mysql中获取papers, paer_auther, paper_reference, authors四个表的field子数据，并保存到本地文件
####################################################################################

def get_data_from_table_concurrent(table_name, key='paperID', data=papers):
    print(f'# Getting {table_name}({key}) from MAG')
    t = time.time()
    db = pd.DataFrame()

    def _query(pair):
        MAG_group, index, pbar = pair
        engine = create_engine(f'mysql+pymysql://{userpass}@192.168.0.140:3306/MACG')
        sql=f'''select * from {table_name} where '''\
              + key + ' in ('+','.join([f'\'{x}\'' for x in MAG_group])+')'
        # time = datetime.now().strftime('%H:%M:%S')
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

print('# getting papers from MAG', datetime.now().strftime('%H:%M:%S'))
df_papers = get_data_from_table_concurrent('papers')
df_papers.to_csv(f'out/{field}/csv/papers.csv',index=False)

df_paper_author = get_data_from_table_concurrent('paper_author')
authors=df_paper_author['authorID'].drop_duplicates().values
df_paper_author.to_csv(f'out/{field}/csv/paper_author.csv',index=False)

citing_db = get_data_from_table_concurrent('paper_reference', key='citingpaperID')
cited_db = get_data_from_table_concurrent('paper_reference', key='citedpaperID')
df_paper_reference_MAG = pd.concat([citing_db, cited_db])
print('paper_reference original', df_paper_reference_MAG.shape)
df_paper_reference_MAG=df_paper_reference_MAG.drop_duplicates()
print('paper_reference drop_duplicates', df_paper_reference_MAG.shape)
df_paper_reference_MAG.to_csv(f'out/{field}/csv/paper_reference.csv',index=False)

df_authors = get_data_from_table_concurrent('authors', key='authorID', data=authors)
df_authors.to_csv(f'out/{field}/csv/authors.csv',index=False)


####################################################################################
# to_sql
# 读取四个子表，并上传到mysql。创建表后添加领域子表的mysql索引（例如在scigene_field_field库）
####################################################################################
print('## uploading papers', datetime.now().strftime('%H:%M:%S'))
# df_papers_MAG = pd.read_csv(f'out/{field}/csv/papers.csv')
print(df_papers, df_papers.shape)
df_papers.to_sql('papers_field',con=engine,if_exists='replace',index=False, dtype={"paperID": sqlalchemy.types.NVARCHAR(length=100),\
    "title": sqlalchemy.types.NVARCHAR(length=2000),"ConferenceID": sqlalchemy.types.NVARCHAR(length=15),"JournalID": sqlalchemy.types.NVARCHAR(length=15),\
        "rank":sqlalchemy.types.INTEGER(),"referenceCount":sqlalchemy.types.INTEGER(),"citationCount":sqlalchemy.types.INTEGER(),"PublicationDate":sqlalchemy.types.Date()})

print('## uploading paper_author', datetime.now().strftime('%H:%M:%S'))
# paper_author_MAG = pd.read_csv(f'out/{field}/csv/paper_author.csv')
print(df_paper_author, df_paper_author.shape)
df_paper_author.to_sql('paper_author_field',con=engine,if_exists='replace',index=False, dtype={"paperID": sqlalchemy.types.NVARCHAR(length=15),\
    "authorID": sqlalchemy.types.NVARCHAR(length=15),"authorOrder":sqlalchemy.types.INTEGER()})

print('## uploading paper_reference', datetime.now().strftime('%H:%M:%S'))
# df_paper_reference_MAG = pd.read_csv(f'out/{field}/csv/paper_reference.csv')
print(df_paper_reference_MAG, df_paper_reference_MAG.shape)
df_paper_reference_MAG.to_sql('paper_reference_field',con=engine,if_exists='replace',index=False, dtype={"citingpaperID": sqlalchemy.types.NVARCHAR(length=15),\
    "citedpaperID": sqlalchemy.types.NVARCHAR(length=15)})

print('## uploading authors', datetime.now().strftime('%H:%M:%S'))
# authors_MAG = pd.read_csv(f'out/{field}/csv/authors.csv')
print(df_authors, df_authors.shape)
df_authors.to_sql('authors_field',con=engine,if_exists='replace',index=False, dtype={"authorID": sqlalchemy.types.NVARCHAR(length=15),\
    "name": sqlalchemy.types.NVARCHAR(length=999),"rank":sqlalchemy.types.INTEGER(),"PaperCount":sqlalchemy.types.INTEGER(),"CitationCount":sqlalchemy.types.INTEGER()})

# add index
print('## add index', datetime.now().strftime('%H:%M:%S'))
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


#######################################################################
# update authors_field
# 计算并添加作者在领域内的论文及引用数量，更新作者的引用总数信息
# 通过计算每位作者的引用次数数据，根据 h-index 的定义，计算并更新了每位作者在特定领域内的 h-index 值
#######################################################################
print('updating authors_field')

try_execute("ALTER TABLE authors_field DROP COLUMN PaperCount_field;")
try_execute("ALTER TABLE authors_field DROP COLUMN CitationCount_field;")
try_execute("ALTER TABLE authors_field DROP COLUMN hIndex_field;")

execute('''
ALTER TABLE authors_field ADD PaperCount_field INT DEFAULT 0;
UPDATE authors_field af
JOIN (
    SELECT authorID, COUNT(*) as count_papers
    FROM paper_author_field
    GROUP BY authorID
) tmp ON af.authorID = tmp.authorID
SET af.PaperCount_field = tmp.count_papers;

ALTER TABLE authors_field ADD CitationCount_field INT DEFAULT 0;
UPDATE authors_field af
JOIN (
    SELECT PA.authorID, SUM(P.citationCount) as total_citations
    FROM papers_field as P 
    JOIN paper_author_field as PA on P.paperID = PA.paperID 
    WHERE P.CitationCount >= 0 
    GROUP BY PA.authorID
) tmp ON af.authorID = tmp.authorID
SET af.CitationCount_field = tmp.total_citations;

ALTER TABLE authors_field ADD hIndex_field INT DEFAULT 0;
''')


cursor.close()
conn.close()


