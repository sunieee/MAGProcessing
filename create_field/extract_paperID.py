import pandas as pd
import numpy as np
import sys
from sqlalchemy import create_engine
from tqdm import tqdm
import os
import time
import sqlalchemy
import concurrent.futures
import multiprocessing
import datetime
import json
from utils import field, cursor, field_info
from collections import defaultdict


userpass = f'{os.environ.get("user")}:{os.environ.get("password")}'
engine = create_engine(f'mysql+pymysql://{userpass}@192.168.0.140:3306/MACG', pool_size=20)
GROUP_SIZE = 2000
multiproces_num = 20
paper_ids = set()

###########################################################################
# 根据名称匹配期刊和会议

paper_path = f'out/{field}/papers.txt'
if os.path.exists(paper_path):
    raise "paper already exist!"

if os.path.exists(f'out/{field}/journal_conference_modify.csv'):
    journal_conference = pd.read_csv(f'out/{field}/journal_conference_modify.csv')
else:
    journal_conference = pd.DataFrame(columns=['type', 'original', 'ID', 'name'])

    for journalID in field_info.get('journalID', []):
        sql_data = f'select name from journals where JournalID=\'{journalID}\';'
        journalName = pd.read_sql_query(sql_data, engine)['name'].tolist()[0]
        journal_conference.loc[len(journal_conference)] = {
            'type': 'journalID',
            'original': journalID,
            'ID': journalID,
            'name': journalName
        }

    pattern = {
        'and': ['&'],
        '_': [':', ' -', '-'],
    }
    for original in field_info.get('journal', []):
        journalName = original
        sql_data = f"select JournalID, name from journals where name like '%%{journalName}%%';"
        print('*', sql_data)
        db_data = pd.read_sql_query(sql_data, engine)

        if len(db_data) == 0 :
            for k, v in pattern.items():
                if k in original:
                    for _v in v:
                        journalName = original.replace(k, _v)
                        sql_data = f"select JournalID, name from journals where name like '%%{journalName}%%';"
                        print('*', sql_data)
                        db_data = pd.read_sql_query(sql_data, engine)
                        if len(db_data) > 0:
                            break

        for row in db_data.to_records():
            journal_conference.loc[len(journal_conference)] = {
                'type': 'journal',
                'original': original,
                'ID': row['JournalID'],
                'name': row['name']
            }

        if len(db_data) == 0:
            print(f'{journalName} not found')
            journal_conference.loc[len(journal_conference)] = {
                'type': 'journal',
                'original': original,
                'ID': None,
                'name': None
            }

    # conferenceIDs = set(field_info.get('conferenceID', []))
    for conferenceID in field_info.get('conferenceID', []):
        sql_data = f'select abbreviation, name from conferences where ConferenceID=\'{conferenceID}\';'
        abbreviation, conferenceName = pd.read_sql_query(sql_data, engine).values[0]
        journal_conference.loc[len(journal_conference)] = {
            'type': 'conferenceID',
            'original': conferenceID,
            'ID': conferenceID,
            'name': abbreviation + '_' + conferenceName
        }

    for original in field_info.get('conference', []):
        abbreviation, conferenceName = original.split('_', 1)
        conferenceName = conferenceName.replace('_', ':')
        sql_data = f"select ConferenceID, abbreviation, name from conferences where name like '%%{conferenceName}%%';"
        print('*', sql_data)
        db_data = pd.read_sql_query(sql_data, engine)
        if len(db_data) == 0:
            sql_data = f"select ConferenceID, abbreviation, name from conferences where abbreviation='{abbreviation}';"
            print('*', sql_data)
            db_data = pd.read_sql_query(sql_data, engine)

        for row in db_data.to_records():
            journal_conference.loc[len(journal_conference)] = {
                'type': 'conference',
                'original': original,
                'ID': row['ConferenceID'],
                'name': row['abbreviation'] + '_' + row['name']
            }

        if len(db_data) == 0:
            print(f'{conferenceName} not found')
            journal_conference.loc[len(journal_conference)] = {
                'type': 'conference',
                'original': original,
                'ID': None,
                'name': None
            }

    journal_conference.drop_duplicates(inplace=True)
    journal_conference.to_csv(f'out/{field}/journal_conference.csv', index=False)

id2row = {row['ID']: row for row in journal_conference.to_records()}


####################################################################################
# extract paperID
# 根据领域名称查询fieldID，如： select * from field_of_study where name='field';
####################################################################################

def get_paperID_batch(pair):
    fieldID, offset, ix, pbar, verbose = pair
    engine = create_engine(f'mysql+pymysql://{userpass}@192.168.0.140:3306/MACG')
    sql_data = f'select paperID from papers_field where fieldID=\'{fieldID}\' LIMIT {GROUP_SIZE} OFFSET {offset};'
    # if verbose:
    #     time = datetime.datetime.now().strftime('%H:%M:%S')
    #     print(f'* {time} ' + sql_data)
    db_data = pd.read_sql_query(sql_data, engine)['paperID'].tolist()
    engine.dispose()
    if verbose:
        pbar.n = int(ix)
        pbar.refresh()
    return db_data


def get_field(field):
    try:
        fieldID = int(field)
        cursor.execute(f"SELECT name, paperCount FROM MACG.field_of_study where fieldID='{fieldID}'")
        result = cursor.fetchone()
        fieldName = result[0]
        paperCount = result[1]
    except:
        fieldName = field
        cursor.execute(f"SELECT fieldID, paperCount  FROM MACG.field_of_study where name='{fieldName}'")
        result = cursor.fetchone()
        fieldID = result[0]
        paperCount = result[1]
    return fieldID, fieldName, paperCount


paper_count = pd.DataFrame(columns=['type', 'original', 'ID', 'name', '#paper', '#accumulate', '#addition', 'validRatio'])
def read_papers(fields, parent=None):
    verbose = parent is None
    for field in tqdm(fields):
        start_count = len(paper_ids)
        fieldID, fieldName, paperCount = get_field(field)
        group_num = paperCount // GROUP_SIZE + 5
        pbar = tqdm(total=group_num)
        print(f'filedID: {fieldID}, paperCount: {paperCount}, group_num: {group_num}')
        with concurrent.futures.ThreadPoolExecutor(max_workers=multiproces_num) as executor:
            results = executor.map(get_paperID_batch, [(fieldID, i*GROUP_SIZE, i, pbar, verbose) for i in range(group_num)])
        
        # 将所有结果合并
        db_data = set()
        for result in tqdm(results):
            db_data.update(result)

        paper_ids.update(db_data)
        paper_count.loc[len(paper_count)] = {
            'type': 'field' if verbose else 'children',
            'original': field if verbose else parent,
            'ID': fieldID,
            'name': fieldName,
            '#paper': len(db_data),
            '#accumulate': len(paper_ids),
            '#addition': len(paper_ids) - start_count,
            'validRatio': (len(paper_ids) - start_count) / len(db_data)
        }
        if verbose:
            print(f'finish reading paperID on {fieldName}({fieldID}), #paper: {len(db_data)}, #accumulate: {len(paper_ids)}')


read_papers(field_info.get('fieldID',[]))
read_papers(field_info.get('field', []))
print(f'## finish reading paperID on field:', len(paper_ids))


for field in field_info.get('children', []):
    fieldID, fieldName, paperCount = get_field(field)
    sql_data = f'select childrenID FROM field_children where parentID=\'{fieldID}\';'
    children_fields = pd.read_sql_query(sql_data, engine).values.ravel().tolist()
    print('*', sql_data, len(children_fields))
    read_papers(children_fields, parent=fieldName)
    print(f'finish reading paperID on children of {fieldName}({fieldID}), #accumulate: {len(paper_ids)}')
print(f'## finish reading paperID on children:', len(paper_ids))


journalIDs = set(journal_conference[journal_conference['type'].isin(['journalID', 'journal'])]['ID'].tolist())
journalIDs = [j for j in journalIDs if str(j) != 'nan' and str(j) != 'None']
for journalID in tqdm(journalIDs):
    sql_data = f'select paperID from papers where JournalID=\'{journalID}\';'
    print('*', sql_data)
    start_count = len(paper_ids)
    db_data = pd.read_sql_query(sql_data, engine)['paperID'].tolist()
    paper_ids.update(db_data)
    paper_count.loc[len(paper_count)] = {
        'type': id2row[journalID]['type'],
        'original': id2row[journalID]['original'], 
        'ID': journalID,
        'name': id2row[journalID]['name'],
        '#paper': len(db_data),
        '#accumulate': len(paper_ids),
        '#addition': len(paper_ids) - start_count,
        'validRatio': (len(paper_ids) - start_count) / len(db_data) if len(db_data) > 0 else 0
    }
    print(f'finish reading paperID on Journal {journalID}, single: {len(db_data)}, all: {len(paper_ids)}')
print(f'## finish reading paperID on Journal:', len(paper_ids))


conferenceIDs = set(journal_conference[journal_conference['type'].isin(['conferenceID', 'conference'])]['ID'].tolist())
# remove None and nan
conferenceIDs = [c for c in conferenceIDs if str(c) != 'nan' and str(c) != 'None']
for conferenceID in tqdm(conferenceIDs):
    sql_data = f'select paperID from papers where ConferenceID=\'{conferenceID}\';'
    print('*', sql_data)
    db_data = pd.read_sql_query(sql_data, engine)['paperID'].tolist()
    paper_ids.update(db_data)
    paper_count.loc[len(paper_count)] = {
        'type': id2row[conferenceID]['type'],
        'original': id2row[conferenceID]['original'],
        'ID': conferenceID,
        'name': id2row[conferenceID]['name'],
        '#paper': len(db_data),
        '#accumulate': len(paper_ids),
        '#addition': len(paper_ids) - start_count,
        'validRatio': (len(paper_ids) - start_count) / len(db_data) if len(db_data) > 0 else 0
    }
    print(f'finish reading paperID on Conference {conferenceID}, single: {len(db_data)}, all: {len(paper_ids)}')
print(f'## finish reading paperID on Conference:', len(paper_ids))

papers = list(paper_ids)
print('# finish reading MAG list from sql, saving to txt', len(papers))

# save list(MAG) to a txt file
np.savetxt(paper_path, papers, fmt='%s')
print('# finish saving MAG list to txt file')

paper_count.to_csv(f'out/{field}/paper_count.csv', index=False)