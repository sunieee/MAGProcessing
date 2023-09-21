import pandas as pd
from utils import *

match_df = pd.read_csv('out/match.csv', index_col=0)

################################################################
# 从后往前merge，只会删除后面的。这样在多个相同name情况下也能正常融合
# 修改领域数据库：
# 1. paper_author_field: 修改author_id
# 2. 重新计算 hIndex_field
# 3. authors_field: 重新刷新一下统计信息：#paper, #citation, hIndex
#   使用UPDATE JOIN语句将id2的值加到id1上，删除id2的记录
################################################################
for i in list(match_df.index)[::-1]:
    id1 = match_df.loc[i]['id1']
    id2 = match_df.loc[i]['id2']
    name1 = match_df.loc[i]['name1']
    name2 = match_df.loc[i]['name2']
    print('=' * 20)
    print(f'merging authors: {name1}({id2}) -> {name2}({id1})')

    execute(f"""UPDATE scigene_{fieldName}_field.paper_author_field
SET authorID = '{id1}'
WHERE authorID = '{id2}';
""")

    execute(f"DELETE FROM scigene_{fieldName}_field.authors_field WHERE authorID = '{id2}';")

#     cursor.execute("""select P.CitationCount from papers_field as P 
#                    join paper_author_field as PA 
#                    on PA.authorID = %s and P.paperID = PA.paperID;""", id1)
#     citations = [int(citation_row[0]) for citation_row in cursor.fetchall()]
#     citations.sort(reverse=True)
#     hIndex_field = sum(1 for i, citation in enumerate(citations) if citation > i)


#     execute(f"""
# UPDATE scigene_{fieldName}_field.authors_field AS a1
# JOIN scigene_{fieldName}_field.authors_field AS a2 ON a1.authorID = '{id1}' AND a2.authorID = '{id2}'
# SET 
#     a1.PaperCount = a1.PaperCount + a2.PaperCount,
#     a1.CitationCount = a1.CitationCount + a2.CitationCount,
#     a1.PaperCount_field = a1.PaperCount_field + a2.PaperCount_field,
#     a1.CitationCount_field = a1.CitationCount_field + a2.CitationCount_field,
#     a1.hIndex_field = {hIndex_field};

# DELETE FROM scigene_{fieldName}_field.authors_field WHERE authorID = '{id2}';
# """)



