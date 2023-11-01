import pandas as pd
from utils import *
from tqdm import tqdm

match_df = pd.read_csv(f'out/{database}/filtered.csv')

################################################################
# 从后往前merge，只会删除后面的。这样在多个相同name情况下也能正常融合
# 修改领域数据库：
# 1. paper_author_field: 修改author_id
# 2. 重新计算 hIndex_field
# 3. authors_field: 重新刷新一下统计信息：#paper, #citation, hIndex
#   使用UPDATE JOIN语句将id2的值加到id1上，删除id2的记录
################################################################
authorIDs = set()
for i in list(match_df.index)[::-1]:
    id1 = match_df.loc[i]['id1']
    id2 = match_df.loc[i]['id2']
    name1 = match_df.loc[i]['name1']
    name2 = match_df.loc[i]['name2']
    print('=' * 20)
    print(f'merging authors: {name2}({id2}) -> {name1}({id1})')

    execute(f"""UPDATE {database}.paper_author_field
SET authorID = '{id1}'
WHERE authorID = '{id2}';
""")

    execute(f"DELETE FROM {database}.authors_field WHERE authorID = '{id2}';")

    authorIDs.add(id1)

authorIDs_str = ', '.join([f"'{x}'" for x in authorIDs])
repeatCondition = f"authorID IN ({authorIDs_str})"

#######################################################################
# update authors_field (局部更新用户信息)
# 计算并添加作者在领域内的论文数量及排名，更新作者的引用总数信息
#######################################################################
print('updating authors_field')
try:
    execute("drop table authors_field_tmp")
except:
    pass

execute(f'''
UPDATE authors_field af
JOIN (
    SELECT authorID, COUNT(*) as count_papers
    FROM paper_author_field 
    WHERE {repeatCondition}
    GROUP BY authorID
) tmp ON af.authorID = tmp.authorID
SET af.PaperCount_field = tmp.count_papers;

UPDATE authors_field af
JOIN (
    SELECT PA.authorID, SUM(P.citationCount) as total_citations
    FROM papers_field as P 
    JOIN paper_author_field as PA on P.paperID = PA.paperID 
    WHERE P.CitationCount >= 0 AND PA.{repeatCondition}
    GROUP BY PA.authorID
) tmp ON af.authorID = tmp.authorID
SET af.CitationCount_field = tmp.total_citations;
''')


#######################################################################
# calculate hIndex （对于合并后的作者计算）
# 通过计算每位作者的引用次数数据，根据 h-index 的定义，计算并更新了每位作者在特定领域内的 h-index 值
# 以反映其影响力和论文引用分布情况。
#######################################################################
for authorID in tqdm(authorIDs):
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



