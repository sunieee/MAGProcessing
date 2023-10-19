from utils import *
import pandas as pd
import os
import time
from tqdm import tqdm
import multiprocessing
from collections import defaultdict
import math
import datetime
import json

paper_dir = f'out/{database}/papers_unprocessed'
link_dir = f'out/{database}/links'
file_list = os.listdir(paper_dir)

with open(f'out/{database}/paperID2abstract.json', 'r') as f:
    paperID2abstract = json.load(f)

def extract_paper_authors(pairs):
    papers, info = pairs
    print('extract_paper_authors', len(papers), info)
    conn = pymysql.connect(host='localhost',
                            port=3306,
                            user='root',
                            password='root',
                            db=database,
                            charset='utf8')
    cursor = conn.cursor()
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

    cursor.close()
    conn.close()
    return _paperID2authorsName


def extract_paper_venu(papers):
    conn = pymysql.connect(host='localhost',
                            port=3306,
                            user='root',
                            password='root',
                            db=database,
                            charset='utf8')
    cursor = conn.cursor()
    _paperID2venue = {}
    for paperID in tqdm(papers):
        cursor.execute(f"select ConferenceID, JournalID from papers_field where paperID='{paperID}'")
        result = cursor.fetchone()
        venu = None
        if result[0] != '0':
            cursor.execute("select abbreviation, name from MACG.conferences where conferenceID=%s", (result[0],))
            res = cursor.fetchone()
            if res != None:
                venu = res[1] + ' (' + res[0] + ')'
        elif result[1] != '0':
            cursor.execute("select name from MACG.journals where journalID=%s", (result[1],))
            res = cursor.fetchone()
            if res != None:
                venu = res[0]
        _paperID2venue[paperID] = venu

    cursor.close()
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
    papers['venu'] = papers['paperID'].apply(lambda paperID: paperID2venue[paperID])
    papers['authorsName'] = papers['paperID'].apply(lambda paperID: paperID2authorsName[paperID])
    papers['abstract'] = papers['paperID'].apply(lambda paperID: paperID2abstract.get(paperID, ''))

    papers.to_csv(filepath.replace('_unprocessed', ''), index=False)

multiproces_num = 20
with multiprocessing.Pool(processes=multiproces_num) as pool:
    pool.map(extract_paper, file_list)

cursor.close()
conn.close()