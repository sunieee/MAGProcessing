# 该代码isKeyPaper仍为0
from utils import *
import pandas as pd
from tqdm import tqdm

# 选取前NUMTOPAUTHORS个学者
cursor.execute(f"""select authorID, name, authorRank 
               from scigene_{fieldName}_field.authors_field 
               where {filterCondition}
""")

rows = cursor.fetchall()


# create mapping (A.authorID: A.name, A=scigene_visualization_field.authors_field) to accelerate
t = time.time()
authorID2name_df = pd.read_sql_query(f"select authorID, name from scigene_{fieldName}_field.authors_field", conn)
authorID2name = authorID2name_df.set_index('authorID')['name'].to_dict()

paperID2FirstAuthorID_df = pd.read_sql_query(f"select paperID, authorID from scigene_{fieldName}_field.paper_author_field where authorOrder = 1", conn)
paperID2FirstAuthorID = paperID2FirstAuthorID_df.set_index('paperID')['authorID'].to_dict()
print('[ create mapping time cost: ', time.time()-t, ']')

# process each author
for authorID, authorName, rank in rows:
    authorID = authorID.strip()
    authorName = authorName.strip()
    rank = int(rank)

    authorTableName = "".join(filter(str.isalpha, authorName)).lower() + str(rank)

    # drop arc author db
    dropfieldPapers_author = f"drop table papers_{authorTableName}"
    try:
        cursor.execute(dropfieldPapers_author)
        conn.commit()
    except Exception as e:
        print("No such table:", dropfieldPapers_author)
    
    #########################################################################
    # 创建表papers_xiaofengli1：paperID, title, year, referenceCount, citationCount, authorOrder, isKeyPaper, firstAuthorID, firstAuthorName
    # 表paper_author_field中authorOrder = 1的authorID为firstAuthorID
    # 更新firstAuthorName
    #########################################################################
    execute(f"""
create table papers_{authorTableName} (firstAuthorID varchar(15), firstAuthorName varchar(999), isKeyPaper float) 
    select papers_field.paperID, title, year, referenceCount, citationCount, min(authorOrder) as authorOrder, 
    0 as isKeyPaper, '' as firstAuthorID, '' as firstAuthorName from scigene_{fieldName}_field.paper_author_field, 
    scigene_{fieldName}_field.papers_field where authorID = '{authorID}' and papers_field.paperID = paper_author_field.paperID 
    group by papers_field.paperID, title, year;
            
create index arc_index on papers_{authorTableName}(paperID);
""")
    # 1. 从数据库中读取papers_danielakeim6表
    t = time.time()
    df = pd.read_sql(f"SELECT * FROM papers_{authorTableName}", conn)
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
        UPDATE papers_{authorTableName}
        SET firstAuthorID = %s, firstAuthorName = %s
        WHERE paperID = %s
        """
        cursor.execute(sql, (row['firstAuthorID'], row['firstAuthorName'], row['paperID']))

    conn.commit()
    print('[ to_sql time cost: ', time.time()-t, ']')
    
    df.to_csv(f'data/{fieldName}/papers_{authorID}.csv', index=False)
    print(f"Create and fill papers for field author {authorName}  with rank {rank}: {authorTableName}")
    
    dropfieldLinks_author = f"drop table links_{authorTableName}"
    try:
        cursor.execute(dropfieldLinks_author)
        conn.commit()
    except Exception as e:
        print("No such table:", dropfieldLinks_author)

    #########################################################################
    # insert potential influence link table for the author
    # 创建links_xiaofengli1：citingpaperID, citedpaperID, sharedAuthor, extends_prob。其中citingpaperID均来自papers_xiaofengli1
    # citingpaperID和citedpaperID均属于xiaofengli1，则sharedAuthor=1
    # 从paper_reference_field_labeled更新extends_prob
    #########################################################################
    execute(f"""
create table links_{authorTableName} (extends_prob float) 
    select P.citingpaperID, P.citedpaperID, 0 as sharedAuthor, null as extends_prob 
    from scigene_{fieldName}_field.paper_reference_field as P 
    where P.citingpaperID in (select paperID from papers_{authorTableName}) 
    group by P.citingpaperID, P.citedpaperID;

create index citing_index on links_{authorTableName}(citingpaperID);
create index cited_index on links_{authorTableName}(citedpaperID);

update links_{authorTableName} as P, scigene_{fieldName}_field.paper_author_field as A, 
    scigene_{fieldName}_field.paper_author_field as B set P.sharedAuthor = 1 
    where A.paperID = P.citingpaperID and B.paperID = P.citedpaperID and A.authorID = B.authorID;

update links_{authorTableName}, scigene_{fieldName}_field.paper_reference_field_labeled 
    set links_{authorTableName}.extends_prob = paper_reference_field_labeled.extends_prob 
    where links_{authorTableName}.citingpaperID = paper_reference_field_labeled.citingpaperID 
    and links_{authorTableName}.citedpaperID = paper_reference_field_labeled.citedpaperID;
""")
    
    df = pd.read_sql(f"SELECT * FROM links_{authorTableName}", conn)
    df.to_csv(f'data/{fieldName}/links_{authorID}.csv', index=False)
    print(f"Create and fill links for field author {authorName} with rank {rank}: {authorTableName}")

cursor.close()
conn.close()