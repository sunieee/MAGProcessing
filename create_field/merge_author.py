import pandas as pd
from utils import *
from tqdm import tqdm

match_df = pd.read_csv(f'out/{database}/filtered.csv', index_col=0)

################################################################
# 从后往前merge，只会删除后面的。这样在多个相同name情况下也能正常融合
# 修改领域数据库：
# 1. paper_author_field: 修改author_id
# 2. 重新计算 hIndex_field
# 3. authors_field: 重新刷新一下统计信息：#paper, #citation, hIndex
#   使用UPDATE JOIN语句将id2的值加到id1上，删除id2的记录
################################################################
authorID_list = []
for i in list(match_df.index)[::-1]:
    id1 = match_df.loc[i]['id1']
    id2 = match_df.loc[i]['id2']
    name1 = match_df.loc[i]['name1']
    name2 = match_df.loc[i]['name2']
    print('=' * 20)
    print(f'merging authors: {name1}({id2}) -> {name2}({id1})')

    execute(f"""UPDATE {database}.paper_author_field
SET authorID = '{id1}'
WHERE authorID = '{id2}';
""")

    execute(f"DELETE FROM {database}.authors_field WHERE authorID = '{id2}';")

    authorID_list.append(id1)

filterCondition = f"authorID IN ({', '.join(map(str, authorID_list))})"

#######################################################################
# update authors_field (局部更新用户信息)
# 计算并添加作者在领域内的论文数量及排名，更新作者的引用总数信息
# 添加 h-index 信息，创建名为 paper_reference_field_labeled 的表，添加名为 FellowType 列
#######################################################################
print('updating authors_field')
try:
    execute("drop table authors_field_tmp")
except:
    pass

execute(f'''
CREATE TABLE authors_field_tmp 
SELECT authorID, COUNT(*) as PaperCount_field 
FROM paper_author_field 
WHERE {filterCondition}
GROUP BY authorID 
ORDER BY PaperCount_field DESC;
        
UPDATE authors_field, authors_field_tmp 
SET authors_field.PaperCount_field = authors_field_tmp.PaperCount_field
WHERE authors_field.authorID = authors_field_tmp.authorID;

drop table authors_field_tmp;

create table authors_field_tmp
    select sum(P.citationCount) as CitationCount_field, authorID 
    from papers_field as P 
        join paper_author_field as PA 
        on P.paperID = PA.paperID and P.CitationCount >=0 
        where PA.{filterCondition}
    group by authorID;

UPDATE authors_field, authors_field_tmp 
SET authors_field.CitationCount_field = authors_field_tmp.CitationCount_field 
WHERE authors_field.authorID = authors_field_tmp.authorID;
''')


#######################################################################
# calculate hIndex
# 通过计算每位作者的引用次数数据，根据 h-index 的定义，计算并更新了每位作者在特定领域内的 h-index 值
# 以反映其影响力和论文引用分布情况。
#######################################################################
# process each author
filterCondition = "PaperCount_field > 10"
authorID_list = pd.read_sql(f"SELECT authorID FROM authors_field WHERE {filterCondition}", conn)['authorID'].tolist()

for authorID in tqdm(authorID_list):
    cursor.execute(f"""select P.CitationCount from papers_field as P 
                   join paper_author_field as PA 
                   on PA.authorID = '{authorID}' and P.paperID = PA.paperID;""")
    rows = cursor.fetchall()
    citations = [int(citation_row[0]) for citation_row in rows]
    citations.sort(reverse=True)
    hIndex_field = sum(1 for i, citation in enumerate(citations) if citation > i)

    cursor.execute(
        "update authors_field set hIndex_field = %s where authorID = %s",
        (hIndex_field, authorID)
    )
    conn.commit()
    # print("Process author: ", authorName, " with rank ", str(authorRank))

cursor.close()
conn.close()



