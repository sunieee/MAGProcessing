import pandas as pd
import os

# 读取 CSV 文件
field = os.environ.get('field')

match_df = pd.read_csv(f'out/{field}/match_modify.csv')
df_paper_author = pd.read_csv(f'out/{field}/csv/paper_author.csv')
df_authors = pd.read_csv(f'out/{field}/csv/authors.csv')
df_papers = pd.read_csv(f'out/{field}/csv/papers.csv')

################################################################
# 从后往前merge，只会删除后面的。这样在多个相同name情况下也能正常融合
# 修改领域数据库：
# 1. paper_author_field: 修改author_id
# 2. 重新计算 hIndex_field
# 3. authors_field: 重新刷新一下统计信息：#paper, #citation, hIndex
#   使用UPDATE JOIN语句将id2的值加到id1上，删除id2的记录
################################################################

# 更新 paper_author.csv
authorIDs = set()
for i in list(match_df.index)[::-1]:
    id1 = match_df.loc[i]['id1']
    id2 = match_df.loc[i]['id2']
    name1 = match_df.loc[i]['name1']
    name2 = match_df.loc[i]['name2']
    print('=' * 20)
    print(f'merging authors: {name2}({id2}) -> {name1}({id1})')
    # 更新 authorID
    df_paper_author.loc[df_paper_author['authorID'] == id2, 'authorID'] = id1
    # 删除 df_authors 中的相关行
    df_authors = df_authors[df_authors['authorID'] != id2]
    authorIDs.add(id1)
    

authorIDs_str = ', '.join([f"'{x}'" for x in authorIDs])
repeatCondition = f"authorID IN ({authorIDs_str})"

# 更新 authors.csv
# 更新 PaperCount_field
paper_count = df_paper_author[df_paper_author['authorID'].isin(authorIDs)].groupby('authorID').size()
df_authors.set_index('authorID', inplace=True)
df_authors.loc[paper_count.index, 'PaperCount_field'] = paper_count.values
df_authors.reset_index(inplace=True)

# 更新 CitationCount_field 和 hIndex_field
for authorID in authorIDs:
    # 这里假设你有一个单独的 papers_field DataFrame
    author_papers = df_paper_author[df_paper_author['authorID'] == authorID]
    citations = df_papers[df_papers['paperID'].isin(author_papers['paperID'])]['citationCount']
    total_citations = citations.sum()
    df_authors.loc[df_authors['authorID'] == authorID, 'CitationCount_field'] = total_citations

    # 创建 citations 的副本并进行排序
    sorted_citations = citations.copy().sort_values(ascending=False)
    hIndex_field = sum(1 for i, citation in enumerate(sorted_citations) if citation > i)
    df_authors.loc[df_authors['authorID'] == authorID, 'hIndex_field'] = hIndex_field

# 将更新后的 DataFrame 写回 CSV 文件
df_paper_author.to_csv(f'out/{field}/csv/paper_author.csv', index=False)
df_authors.to_csv(f'out/{field}/csv/authors.csv', index=False)
