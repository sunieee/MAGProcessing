import sys
import time
import pandas as pd
from tqdm import tqdm
from utils import *


####################################################################################
# create timeseries raw
# 从论文引用和发布日期信息中创建了一个名为papers_field_citation_timeseries_raw的数据表，记录了每篇论文每年的引用次数，同时去除了无效的年份数据。
####################################################################################
# 创建 papers_field_citation_timeseries_raw 表
print('creating papers_field_citation_timeseries_raw_raw')
sql = '''
SELECT M.paperID, R.citingpaperID, 0 as year
FROM papers_field AS M
JOIN paper_reference_field AS R ON M.paperID = R.citedpaperID
'''
papers_field_citation_timeseries_raw_raw = pd.read_sql_query(sql, conn)
macg_papers = pd.read_csv(f'out/{database}/papers.csv', usecols=['paperID', 'PublicationDate'], dtype={'paperID': str, 'PublicationDate': str})
macg_papers['PublicationDate'] = pd.to_datetime(macg_papers['PublicationDate'])


# 创建 MACG_papers_tmpid 表
print('creating MACG_papers_tmpid')
macg_papers_tmpid = papers_field_citation_timeseries_raw_raw['citingpaperID'].drop_duplicates()

# 创建 MACG_papers_tmp 表
print('creating MACG_papers_tmp')
macg_papers_tmp = pd.merge(macg_papers_tmpid, macg_papers, left_on='citingpaperID', right_on='paperID')

# 更新引用时间序列的 year 列
print('updating year')
macg_papers_tmp['year'] = macg_papers_tmp['PublicationDate'].dt.year
papers_field_citation_timeseries_raw_raw['year'] = macg_papers_tmp['year']

# 创建 papers_field_citation_timeseries_raw 表
print('creating papers_field_citation_timeseries_raw')
papers_field_citation_timeseries_raw = papers_field_citation_timeseries_raw_raw.groupby(['paperID', 'year']).size().reset_index(name='cited_cnt')
# 删除年份小于等于 0 的行
print('deleting rows with year <= 0')
papers_field_citation_timeseries_raw = papers_field_citation_timeseries_raw[papers_field_citation_timeseries_raw['year'] > 0]


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

# select all raw paper years
print('selecting all raw paper years')
cursor.execute("select paperID, year(PublicationDate) from papers_field")
rows = cursor.fetchall()

paperYearMap = {}

for row in rows:
    paperID = str(row[0].strip())
    try:
        year = int(row[1])
    except:
        # 部分数据 PublicationDate 为空
        print("year error: ", row[1], ", paper ID: ", paperID)
        year = 0
    paperYearMap[paperID] = year

# citationCountMap[paperID][year] = count
citationCountMap = {}

# process each citation count
print('processing each citation count')
MIN_YEAR = 1901
MAX_YEAR = MAX_CITATION_YEAR = 2022

for index, row in papers_field_citation_timeseries_raw.iterrows():
    paperID = str(row['paperID'].strip())
    year = int(row['year'])
    citationCount = int(row['cited_cnt'])

    if (year < MIN_YEAR) or (year > MAX_YEAR):
        print("citation year out of range: ", year, ", paper ID: ", paperID)
        continue

    if not (paperID in citationCountMap):
        citationCountMap[paperID] = {}

    citationCountMap[paperID][year] = citationCount


print('inserting into papers_field_citation_timeseries')
for paperID in tqdm(citationCountMap):

    publicationYear = paperYearMap[paperID]
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
            citationCountByYear += ",0" * (current_year - last_year - 1)

        citationCountByYear += "," + str(citationYearMap[current_year])

        last_year = current_year

    values = (paperID, publicationYear, citeStartYear, citeEndYear, totalCitationCount, citationCountByYear)
    cursor.execute(
        "insert into papers_field_citation_timeseries values(%s, %s, %s, %s, %s, %s)",
        values
    )

conn.commit()


####################################################################################
# update papers_field
# 将论文表中的年份更新为发布日期的年份，为年份添加索引，然后从papers_field_citation_timeseries表复制引用次数序列数据到新的列中。
####################################################################################
print('updating papers_field')
cursor.execute("SHOW COLUMNS FROM papers_field LIKE 'year'")
if cursor.fetchone() is None:
    cursor.execute("ALTER TABLE papers_field ADD year INT")
    conn.commit()

execute('''
update papers_field set year = year(PublicationDate);
alter table papers_field add index(year);

ALTER TABLE papers_field ADD citationCountByYear varchar(999);
update papers_field as PA, papers_field_citation_timeseries as PM set PA.citationCountByYear = PM.citationCountByYear where PA.paperID = PM.paperID;
''')
