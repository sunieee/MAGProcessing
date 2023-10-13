# 该代码isKeyPaper仍为0
from utils import *
import pandas as pd
from tqdm import tqdm
import math

# create mapping (A.authorID: A.name, A=scigene_visualization_field.authors_field) to accelerate
t = time.time()
authorID2name_df = pd.read_sql_query(f"select authorID, name from {database}.authors_field", conn)
authorID2name = authorID2name_df.set_index('authorID')['name'].to_dict()

paperID2FirstAuthorID_df = pd.read_sql_query(f"select paperID, authorID from {database}.paper_author_field where authorOrder = 1", conn)
paperID2FirstAuthorID = paperID2FirstAuthorID_df.set_index('paperID')['authorID'].to_dict()
print('[ create mapping time cost: ', time.time()-t, ']')


# process each author
print('## start to process each author (create papers)', len(authorID_list))

def build_top_author(pairs):
    authorID_list, order = pairs
    print(order, len(authorID_list))
    conn = pymysql.connect(host='localhost',
                            port=3306,
                            user='root',
                            password='root',
                            db=database_pcg,
                            charset='utf8')
    cursor = conn.cursor()
    
    for authorID in tqdm(authorID_list):
        try_execute(f"drop table papers_{authorID}", cursor=cursor)
        
        #########################################################################
        # 创建表papers_xiaofengli1：paperID, title, year, referenceCount, citationCount, authorOrder, isKeyPaper, firstAuthorID, firstAuthorName
        # 表paper_author_field中authorOrder = 1的authorID为firstAuthorID
        # 更新firstAuthorName
        #########################################################################
        t = time.time()
        execute(f"""
    create table papers_{authorID} (firstAuthorID varchar(15), firstAuthorName varchar(999), isKeyPaper float) 
        select papers_field.paperID, title, year, referenceCount, citationCount, min(authorOrder) as authorOrder, 
        0 as isKeyPaper, '' as firstAuthorID, '' as firstAuthorName 
        from {database}.paper_author_field, 
        {database}.papers_field where authorID = '{authorID}' and papers_field.paperID = paper_author_field.paperID 
        group by papers_field.paperID, title, year;
                
    create index arc_index on papers_{authorID}(paperID);
    """, cursor=cursor)
        # 1. 从数据库中读取papers_danielakeim6表
        # t = time.time()
        df = pd.read_sql(f"SELECT * FROM papers_{authorID}", conn)
        print('[ read_sql time cost: ', time.time()-t, ']')

        # 2. 使用map方法和authorID2name字典更新firstAuthorName字段
        t = time.time()
        df['firstAuthorID'] = df['paperID'].map(paperID2FirstAuthorID)
        print('[ map time cost1: ', time.time()-t, ']')
        df['firstAuthorName'] = df['firstAuthorID'].map(authorID2name)
        print('[ map time cost: ', time.time()-t, ']')

        # 3. 将更新后的数据写回数据库
        # df.to_csv('test.csv')
        t = time.time()
        df = df.where(pd.notna(df), None)
        for i in range(len(df)):
            row = df.iloc[i]
            sql = f"""
            UPDATE papers_{authorID}
            SET firstAuthorID = %s, firstAuthorName = %s
            WHERE paperID = %s
            """
            cursor.execute(sql, (row['firstAuthorID'], row['firstAuthorName'], row['paperID']))

        conn.commit()
        # print('[ to_sql time cost: ', time.time()-t, ']')

    cursor.close()
    conn.close()

multiproces_num=20
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(build_top_author, [(authorID_list[i::multiproces_num], f'{i}/{multiproces_num}') for i in range(multiproces_num)])

cursor.close()
conn.close()
