from tqdm import tqdm
from utils import *
import pandas as pd


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

UPDATE authors_field
SET authorRank = 0
WHERE {filterCondition} and authorRank = -1;
''')

#######################################################################
# calculate hIndex
# 通过计算每位作者的引用次数数据，根据 h-index 的定义，计算并更新了每位作者在特定领域内的 h-index 值
# 以反映其影响力和论文引用分布情况。
#######################################################################
# process each author
for authorID in authorID_list:
    rows = executeFetch(f"""select P.CitationCount from papers_field as P 
                   join paper_author_field as PA 
                   on PA.authorID = '{authorID}' and P.paperID = PA.paperID;""")
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