
import pymysql
import pandas as pd
import os
import time
from tqdm import tqdm
import multiprocessing
from collections import defaultdict
import math
import datetime
import json
from utils import create_connection, database, original_dir

paper_dir = f'{original_dir}/papers'
file_list = os.listdir(paper_dir)

with open(f'{original_dir}/paperID2abstract.json', 'r') as f:
    paperID2abstract = json.load(f)

df_papers_field = pd.read_csv(f'{original_dir}/csv/papers_field.csv')
df_papers_field['paperID'] = df_papers_field['paperID'].astype(str)
paperID2referenceCount = dict(zip(df_papers_field['paperID'], df_papers_field['referenceCount']))
paperID2citationCount = dict(zip(df_papers_field['paperID'], df_papers_field['citationCount']))

def extract_paper_authors(pairs):
    papers, info = pairs
    print('extract_paper_authors', len(papers), info)
    conn, cursor = create_connection(database)
    _paperID2authorsName = defaultdict(list)

    # 使用IN子句一次查询多个paperID
    paper_ids_str = ', '.join([f"'{x}'" for x in papers])
    cursor.execute(f"""SELECT paper_author_field.paperID, authors_field.name
                       FROM paper_author_field 
                       JOIN authors_field ON paper_author_field.authorID=authors_field.authorID 
                       WHERE paper_author_field.paperID IN ({paper_ids_str})
                       ORDER BY paper_author_field.paperID, paper_author_field.authorOrder;""")
    result = cursor.fetchall()

    # 使用Python代码来组合结果
    for paperID, name in result:
        _paperID2authorsName[paperID].append(name)
    for paperID, names in _paperID2authorsName.items():
        _paperID2authorsName[paperID] = ', '.join(names)
    conn.close()
    return _paperID2authorsName


def valid_venue(venu):
    if venu is None:
        return False
    if venu in ['None', ' ', '', '0']:
        return False
    return True

def extract_paper_venu(papers):
    conn, cursor = create_connection(database)
    _paperID2venue = {}
    for paperID in tqdm(papers):
        cursor.execute(f"select ConferenceID, JournalID from papers_field where paperID='{paperID}'")
        result = cursor.fetchone()
        # print(result)
        venu = None
        if valid_venue(result[0]):
            cursor.execute("select abbreviation, name from MACG.conferences where conferenceID=%s", (result[0],))
            res = cursor.fetchone()
            if valid_venue(res):
                venu = res[1] + ' (' + res[0] + ')'
        elif valid_venue(result[1]):
            cursor.execute("select name from MACG.journals where journalID=%s", (result[1],))
            res = cursor.fetchone()
            if res != None:
                venu = res[0]
        _paperID2venue[paperID] = venu

    conn.close()
    return _paperID2venue

paperID2venue = defaultdict(str)
paperID2authorsName = defaultdict(str)

paperID_list = []
for file in tqdm(file_list):
    filepath = os.path.join(paper_dir, file)
    papers = pd.read_csv(filepath)
    papers['paperID'] = papers['paperID'].astype(str)
    paperID_list += papers["paperID"].values.tolist()
paperID_list = list(set(paperID_list))
print('len(paperID_list)', len(paperID_list), datetime.datetime.now().strftime('%H:%M:%S'))

multiproces_num = 20
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(extract_paper_venu, [paperID_list[i::multiproces_num] for i in range(multiproces_num)])
    for result in results:
        paperID2venue.update(result)
print('finish extract_paper_venu', len(paperID2venue), datetime.datetime.now().strftime('%H:%M:%S'))

group_size = 2000
group_length = math.ceil(len(paperID_list)/group_size)
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(extract_paper_authors, [(paperID_list[i*group_size:i*group_size+group_size], f'{i}/{group_length}') for i in range(group_length)])
    for result in results:
        paperID2authorsName.update(result)
print('finish extract_paper_authors', len(paperID2authorsName), datetime.datetime.now().strftime('%H:%M:%S'))

def extract_paper(file):
    filepath = os.path.join(paper_dir, file)
    papers = pd.read_csv(filepath)
    papers = papers.drop(columns=["authorOrder", "firstAuthorID", "firstAuthorName"])
    papers['paperID'] = papers['paperID'].astype(str)
    papers['referenceCount'] = papers['paperID'].apply(lambda paperID: paperID2referenceCount[paperID])
    papers['citationCount'] = papers['paperID'].apply(lambda paperID: paperID2citationCount[paperID])
    papers['venu'] = papers['paperID'].apply(lambda paperID: paperID2venue[paperID])
    papers['authorsName'] = papers['paperID'].apply(lambda paperID: paperID2authorsName[paperID])
    papers['abstract'] = papers['paperID'].apply(lambda paperID: paperID2abstract.get(paperID, ''))

    papers.to_csv(f'out/{database}/papers/' + filepath.split('/')[-1], index=False)

    core_papers = papers[papers['isKeyPaper'] > 0.5]
    core_citations = core_papers['citationCount'].to_list()
    core_citations.sort(reverse=True)
    return {
        'authorID': file.split('.')[0],
        'CorePaperCount_field': len(core_papers),
        'CoreCitationCount_field': core_papers['citationCount'].sum(),
        'CorehIndex_field': sum(1 for i, citation in enumerate(core_citations) if citation > i)
    }

multiproces_num = 20
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(extract_paper, file_list)

df = pd.DataFrame(results)
top_field_authors = pd.read_csv(f'{original_dir}/top_field_authors.csv')
top_field_authors['authorID'] = top_field_authors['authorID'].astype(str)
top_field_authors = top_field_authors.merge(df, on='authorID')

top_field_authors.to_csv(f'out/{database}/top_field_authors.csv', index=False)