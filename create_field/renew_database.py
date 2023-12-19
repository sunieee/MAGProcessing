from utils import database, cursor, conn
import os
from tqdm import tqdm
import pandas as pd
from datetime import datetime
import json


if not os.path.exists(f'out/{database}/papers.csv'):
    df_papers = pd.read_sql_query(f"""select * from papers_field""", conn)
    df_papers.to_csv(f'out/{database}/papers.csv', index=False)
else:
    df_papers = pd.read_csv(f'out/{database}/papers.csv')
    df_papers['paperID'] = df_papers['paperID'].astype(str)

if not os.path.exists(f'out/{database}/paper_reference.csv'):
    df_paper_reference = pd.read_sql_query(f"""select * from paper_reference_field""", conn)
    df_paper_reference.to_csv(f'out/{database}/paper_reference.csv', index=False)
else:
    df_paper_reference = pd.read_csv(f'out/{database}/paper_reference.csv')
    df_paper_reference['citingpaperID'] = df_paper_reference['citingpaperID'].astype(str)
    df_paper_reference['citedpaperID'] = df_paper_reference['citedpaperID'].astype(str)

if not os.path.exists(f'out/{database}/paper_author.csv'):
    df_paper_author = pd.read_sql_query(f"""select * from paper_author_field""", conn)
    df_paper_author.to_csv(f'out/{database}/paper_author.csv', index=False)
else:
    df_paper_author = pd.read_csv(f'out/{database}/paper_author.csv')
    df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
    df_paper_author['paperID'] = df_paper_author['paperID'].astype(str)

if not os.path.exists(f'out/{database}/authors.csv'):
    df_authors = pd.read_sql_query(f"""select * from authors_field""", conn)
    df_authors.to_csv(f'out/{database}/authors.csv', index=False)
else:
    df_authors = pd.read_csv(f'out/{database}/authors.csv')
    df_authors['authorID'] = df_authors['authorID'].astype(str)

print('load data finished', datetime.now().strftime("%H:%M:%S"))
cnt = 1000000
min_size = 2
while cnt > 200000:
    min_size += 1
    filtered_authors = df_authors[df_authors['PaperCount_field'] >= min_size]
    cnt = len(filtered_authors)

print('min_size:', min_size, 'cnt:', cnt)
authorID_list = filtered_authors['authorID'].tolist()

df_paper_author = df_paper_author[df_paper_author['authorID'].isin(authorID_list)]
print('df_paper_author:', len(df_paper_author), datetime.now().strftime("%H:%M:%S"))

if os.path.exists(f'out/{database}/paperID2citationCount.json'):
    with open(f'out/{database}/paperID2citationCount.json', 'r') as f:
        paperID2citationCount = json.load(f)
else:
    df_papers = df_papers[df_papers['paperID'].isin(df_paper_author['paperID'])]
    print('df_papers:', len(df_papers), datetime.now().strftime("%H:%M:%S"))

    paperID2citationCount = pd.Series(df_papers.citationCount.values, index=df_papers.paperID).to_dict()
    print('paperID2citationCount:', len(paperID2citationCount), datetime.now().strftime("%H:%M:%S"))
    # save paperID2citationCount
    with open(f'out/{database}/paperID2citationCount.json', 'w') as f:
        json.dump(paperID2citationCount, f)

# 对 df_paper_author 按照 authorID 进行分组，并将 paperID 聚合为列表
def calculate_h_index(paperIDs, paperID2citationCount):
    citations = [paperID2citationCount.get(paperID, 0) for paperID in paperIDs]
    citations.sort(reverse=True)
    h_index = sum(1 for i, citation in enumerate(citations) if citation > i)
    return h_index

author_h_index = df_paper_author.groupby('authorID')['paperID'].apply(lambda paperIDs: calculate_h_index(paperIDs, paperID2citationCount))
print('author_h_index:', len(author_h_index), datetime.now().strftime("%H:%M:%S"))

authorID2h_index = author_h_index.to_dict()
# save authorID2h_index
with open(f'out/{database}/authorID2h_index.json', 'w') as f:
    json.dump(authorID2h_index, f)

update_query = "UPDATE authors_field SET hIndex_field = %s WHERE authorID = '%s'"
# cursor.executemany(update_query, h_index_updates)
# conn.commit()

for authorID, h_index in tqdm(authorID2h_index.items()):
    cursor.execute(update_query, (h_index, authorID))