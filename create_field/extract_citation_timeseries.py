import sys
import time
import pandas as pd
from tqdm import tqdm
from utils import database, create_connection, execute, executeFetch, try_execute, cursor, conn, engine
import multiprocessing
import math
import numpy as np
import concurrent.futures
import json

''' 等价mysql命令，但是效率更高
drop table papers_field_citation_timeseries_raw_raw;
create table papers_field_citation_timeseries_raw_raw select M.paperID, R.citingpaperID, 0 as year from papers_field as M join paper_reference_field as R on M.paperID = R.citedpaperID;
create index id_index on papers_field_citation_timeseries_raw_raw(paperID);
create index citingid_index on papers_field_citation_timeseries_raw_raw(citingpaperID);
create index year_index on papers_field_citation_timeseries_raw_raw(year);

create table MACG_papers_tmpid select citingpaperID as paperID from papers_field_citation_timeseries_raw_raw group by citingpaperID;
create index id_index on MACG_papers_tmpid(paperID);
create table MACG_papers_tmp select P.* from MACG.papers as P join MACG_papers_tmpid as D on P.paperID = D.paperID;
create index id_index on MACG_papers_tmp(paperID);

update papers_field_citation_timeseries_raw_raw as C, MACG_papers_tmp as P set C.year = year(P.PublicationDate) where C.citingpaperID = P.paperID;
create table papers_field_citation_timeseries_raw select paperID, year, count(*) as cited_cnt from papers_field_citation_timeseries_raw_raw group by paperID, year;
delete from papers_field_citation_timeseries_raw where year <= 0
'''

####################################################################################
# create timeseries raw
# 从论文引用和发布日期信息中创建了一个名为papers_field_citation_timeseries_raw的数据表，记录了每篇论文每年的引用次数，同时去除了无效的年份数据。
####################################################################################
paper_reference = pd.read_csv(f'out/{database}/paper_reference.csv', dtype={'citingpaperID': str, 'citedpaperID': str})
paper_reference = paper_reference[['citedpaperID', 'citingpaperID']]
paper_reference.columns = ['paperID', 'citingpaperID']

paperID_list = paper_reference['paperID'].unique().tolist() + paper_reference['citingpaperID'].unique().tolist()
paperID_list = list(set(paperID_list))

def extract_paper_year(pair):
    paperID_list, order = pair
    print(f'* extracting paper year: {order}')
    conn, cursor = create_connection(database)
    # 使用IN子句一次查询多个paperID
    macg_papers = pd.read_sql_query(f"""select paperID, year(PublicationDate) as year from MACG.papers 
                                where paperID in {tuple(paperID_list)}""", engine)
    _paperID2year = dict(zip(macg_papers['paperID'], macg_papers['year']))

    cursor.close()
    conn.close()
    return {k: v for k, v in _paperID2year.items() if v}

if os.path.exists(f'out/{database}/paperID2year.json'):
    with open(f'out/{database}/paperID2year.json', 'r') as f:
        paperID2year = json.load(f)
else:
    paperID2year = {}
    multiproces_num = 20
    group_size = 2000
    group_length = math.ceil(len(paperID_list)/group_size)
    with multiprocessing.Pool(processes=multiproces_num) as pool:
        results = pool.map(extract_paper_year, [(paperID_list[i*group_size:(i+1)*group_size], f'{i}/{group_length}') for i in range(group_length)])
        for result in results:
            paperID2year.update(result)
    print('extract paper year done', len(paperID2year))
    with open(f'out/{database}/paperID2year.json', 'w') as f:
        json.dump(paperID2year, f)

paper_reference['year'] = paper_reference['citingpaperID'].apply(lambda x: paperID2year.get(x, 0))
paper_reference = paper_reference[paper_reference['year'] > 0].astype({'year': int})
papers_field_citation_timeseries_raw = paper_reference.groupby(['paperID', 'year']).size().reset_index(name='cited_cnt')

####################################################################################
# create timeseries
# 从raw中提取、处理并创建名为 papers_field_citation_timeseries 的数据表，记录了每篇论文在不同年份的引用次数信息，同时保证年份的连续性和数据的完整性。
####################################################################################
try_execute("drop table papers_field_citation_timeseries;")
cursor.execute("""CREATE TABLE papers_field_citation_timeseries(
paperID varchar(15),
publicationYear int,
citeStartYear int,
citeEndYear int,
totalCitationCount int,
citationCountByYear varchar(999),
PRIMARY KEY (paperID)
);""")
conn.commit()
citationCountMap = {}

# process each citation count
print('processing each citation count')
MIN_YEAR = 1901
MAX_YEAR = MAX_CITATION_YEAR = 2022

range(len(papers_field_citation_timeseries_raw))

def extract_citationCountMap(pair):
    idx_list, order = pair
    print(f'* extracting citationCountMap: {order}')
    citationCountMap = {}
    for i in tqdm(idx_list):
        row = papers_field_citation_timeseries_raw.iloc[i]
        paperID = str(row['paperID'].strip())
        year = int(row['year'])

        if (year < MIN_YEAR) or (year > MAX_YEAR):
            print("citation year out of range: ", year, ", paper ID: ", paperID)
            continue

        if not (paperID in citationCountMap):
            citationCountMap[paperID] = {}
        citationCountMap[paperID][year] = int(row['cited_cnt'])
    return citationCountMap


group_size = 10000
multiproces_num = 20
group_length = math.ceil(len(papers_field_citation_timeseries_raw)/group_size)
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(extract_citationCountMap, [
        (
            range(i * group_size, min((i + 1) * group_size, len(papers_field_citation_timeseries_raw))), 
            f'{i}/{group_length}'
        ) 
        for i in range(group_length)])
    for result in results:
        for k, v in result.items():
            if k not in citationCountMap:
                citationCountMap[k] = v
            else:
                citationCountMap[k].update(v)

print('inserting into papers_field_citation_timeseries')

def insert_citation_timeseries(pair):
    paperID_list, order = pair
    print(f'* inserting into papers_field_citation_timeseries: {order}')
    conn, cursor = create_connection(database)
    for paperID in tqdm(paperID_list):
        if paperID2year.get(paperID, 0) == 0 or np.isnan(paperID2year[paperID]):
            print("paper year not valid: ", paperID, paperID2year.get(paperID, 0))
            continue

        publicationYear = paperID2year[paperID]
        citationYearMap = citationCountMap[paperID]
        yearList = sorted(citationYearMap.keys())

        if (publicationYear < yearList[0]) and (publicationYear >= MIN_YEAR):
            yearList.insert(0, publicationYear)
            citationYearMap[publicationYear] = 0

        if not (MAX_CITATION_YEAR in yearList):
            yearList.append(MAX_CITATION_YEAR)
            citationYearMap[MAX_CITATION_YEAR] = 0

        citeStartYear = yearList[0]
        citeEndYear = yearList[len(yearList) - 1]

        totalCitationCount = citationYearMap[yearList[0]]
        citationCountByYear = str(citationYearMap[yearList[0]])

        last_year = yearList[0]
        current_year = -1

        for index in range(1, len(yearList)):

            current_year = yearList[index]
            totalCitationCount += citationYearMap[current_year]

            if current_year > (last_year + 1):
                citationCountByYear += ",0" * int(current_year - last_year - 1)

            citationCountByYear += "," + str(citationYearMap[current_year])
            last_year = current_year

        values = (paperID, publicationYear, citeStartYear, citeEndYear, totalCitationCount, citationCountByYear)
        cursor.execute(
            "insert into papers_field_citation_timeseries values(%s, %s, %s, %s, %s, %s)",
            values
        )

    conn.commit()
    cursor.close()
    conn.close()

paperID_list = list(citationCountMap.keys())
group_length = math.ceil(len(paperID_list)/group_size)
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(insert_citation_timeseries, [(paperID_list[i*group_size:(i+1)*group_size], f'{i}/{group_length}') for i in range(group_length)])

cursor.close()
conn.close()