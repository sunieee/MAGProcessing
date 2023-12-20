import math
import json
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import multiprocessing

# import all needed variable from utils
from utils import *


df_papers, df_authors, df_paper_author, df_paper_author_filtered, top_authors = create_top()
path_to_mapping = f"out/{field}/csv"
authorID_list = top_authors['authorID'].tolist()


MIN_STUDENT_AUTHOR_ORDER = 3
print("Pre-compute first author maps!", datetime.now().strftime("%H:%M:%S"))
firstAuthorTmp = df_paper_author_filtered.merge(df_paper_author, on="paperID", suffixes=('', '_first')) \
    .query("authorOrder > 1 and authorOrder_first == 1") 
firstAuthorTmp = firstAuthorTmp[['authorID', 'paperID', 'authorOrder', 'authorID_first']]
firstAuthorTmp = firstAuthorTmp.groupby(['authorID', 'authorID_first']).size().reset_index(name='count')
firstAuthors = firstAuthorTmp['authorID_first'].unique().tolist()

'''
        authorID authorID_first  count
0      100461504     2012192335      2
1      100461504     2059806765      1
2      100461504     2263221087      3
3      100461504     2269301939      2
4      100461504     2277984915      1
...          ...            ...    ...
95058   99770489     2625989380      2
95059   99770489     2764676664      2
95060   99770489     2895040106      1
95061   99770489     3109868778      1
95062   99770489      837107254      3

[95063 rows x 3 columns]
'''

######################################################################
# 从数据库中查询领域的**firstAuthor + topAuthor **信息，构建两个映射：
# 1. paperCountMap：映射每个作者ID到一个子映射，其中子映射的键是年份，值是该年份的论文数量。
# 2. weightedPaperCountMap：与上面的映射类似，但值是加权的论文数量，
#       其中权重是1/作者顺序，但仅考虑作者顺序小于或等于MIN_STUDENT_AUTHOR_ORDER的作者。
######################################################################
print("compute first-author maps!", datetime.now().strftime("%H:%M:%S"))
# Perform the necessary merges and group by operations
merged_df = df_paper_author.merge(df_papers, on='paperID')
filtered_df = merged_df[merged_df['authorID'].isin(firstAuthors + authorID_list)]
grouped = filtered_df.groupby(['authorID', 'authorOrder', 'year']).size().reset_index(name='cnt')
paperCountMap = {}
weightedPaperCountMap = {}

for _, row in grouped.iterrows():
    authorID = row['authorID'].strip()
    authorOrder = int(row['authorOrder'])
    year = int(row['year'])
    count = int(row['cnt'])
    # 更新paperCountMap
    yearCountMap = paperCountMap.setdefault(authorID, {})
    yearCountMap[year] = yearCountMap.get(year, 0) + count
    # 更新weightedPaperCountMap
    if authorOrder <= MIN_STUDENT_AUTHOR_ORDER:
        yearWeightedCountMap = weightedPaperCountMap.setdefault(authorID, {})
        yearWeightedCountMap[year] = yearWeightedCountMap.get(year, 0) + count / authorOrder


######################################################################
# 从数据库中查询领域的论文合作者信息，**相关作者（topAuthor）**有关的不是第一作者论文，并基于查询结果构建两个映射：
# 1. coWeightedPaperCountMap：映射每个合作者ID对（由两个作者ID组成）到一个子映射，
#   其中子映射的键是年份，值是加权的论文数量，其中权重是1/作者顺序。
# 2. coPaperCountMap：与上面的映射类似，但值是该年份的论文数量。
######################################################################
print("compute co-author maps!", datetime.now().strftime("%H:%M:%S"))
coauthor_joined = firstAuthorTmp.merge(
    df_paper_author[df_paper_author['authorOrder'] <= MIN_STUDENT_AUTHOR_ORDER], 
    left_on='authorID_first', right_on='authorID', suffixes=('', '_PA1')
).merge(
    df_paper_author, left_on=['authorID', 'paperID'], right_on=['authorID', 'paperID'], suffixes=('', '_PA2')
)
coauthor_joined = coauthor_joined[coauthor_joined['authorOrder'] < coauthor_joined['authorOrder_PA2']]
'''
PA1 是第一作者，PA2 是合作者，查询两者之间有没有其他的联合论文（也需要PA1.order < PA2.order）
                                                        PA1             PA1             PA2
          authorID authorID_first  count     paperID authorID_PA1  authorOrder  authorOrder_PA2
0        100461504     2012192335      2  1510416401   2012192335            1                2
1        100461504     2012192335      2  1486682013   2012192335            1                3
2       2429897830     2012192335      2  1490177788   2012192335            1                2
3       2429897830     2012192335      2  2100876320   2012192335            2                5
4       2429897830     2059806765      1  2100876320   2059806765            1                5
...            ...            ...    ...         ...          ...          ...              ...
240906    99467868     2020717950      2  2498751001   2020717950            1                2
240907    99467868       51191428      1  1438141466     51191428            1                4
240908    99770489     2124762050      1  2036318837   2124762050            1                2
240909    99770489     2327525373      1  2619713135   2327525373            1               10
240910    99770489     2895040106      1  2018431971   2895040106            1                2

[209923 rows x 7 columns]
'''
coauthor_year_joined = coauthor_joined.merge(df_papers[['paperID', 'year']], left_on="paperID", right_on="paperID", suffixes=('', '_P'))
coauthor_year_joined = coauthor_year_joined[['authorID_first', 'authorID', 'paperID', 'authorOrder', 'year']].drop_duplicates()
grouped = coauthor_year_joined.groupby(['authorID_first', 'authorID', 'authorOrder', 'year']).size().reset_index(name='count')
grouped['coAuthorID'] = grouped['authorID_first'] + "-" + grouped['authorID']

coWeightedPaperCountMap = {}
coPaperCountMap = {}

for _, row in grouped.iterrows():
    coAuthorID = row['coAuthorID']
    year = int(row['year'])
    count = row['count']
    authorOrder = row['authorOrder']
    # For coWeightedPaperCountMap
    yearWeightedCountMap = coWeightedPaperCountMap.setdefault(coAuthorID, {})
    yearWeightedCountMap[year] = yearWeightedCountMap.get(year, 0) + count / authorOrder
    # For coPaperCountMap
    yearCountMap = coPaperCountMap.setdefault(coAuthorID, {})
    yearCountMap[year] = yearCountMap.get(year, 0) + count


# save all the maps to {path_to_csv}/*.json
with open(f"{path_to_mapping}/paperCountMap.json", "w") as f:
    json.dump(paperCountMap, f)
with open(f"{path_to_mapping}/weightedPaperCountMap.json", "w") as f:
    json.dump(weightedPaperCountMap, f)
with open(f"{path_to_mapping}/coWeightedPaperCountMap.json", "w") as f:
    json.dump(coWeightedPaperCountMap, f)
with open(f"{path_to_mapping}/coPaperCountMap.json", "w") as f:
    json.dump(coPaperCountMap, f)



MIN_SUPERVISOR_RATE = 0.3
MIN_SUPERVISED_RATE = 1
MIN_SUPERVISING_RATE = 1
MIN_SUPERVISED_YEAR_SPAN = 2
MIN_SUPERVISED_PAPER_SPAN = 2.1
MAX_SUPERVISED_YEAR = 6
HALF_SUPERVISED_YEAR = 3
MAX_YEAR = 1000

MAX_SUPERVISED_PAPER = 10
HALF_SUPERVISED_PAPER = 5
MAX_PAPER = 1000

MAX_ACADEMIC_YEAR = int(
    MAX_SUPERVISED_YEAR
    - 1
    - math.log(MIN_SUPERVISOR_RATE * MIN_SUPERVISED_RATE)
    * HALF_SUPERVISED_YEAR
    / math.log(2)
)

SUPERVISED_YEAR_MODIFIER = []

for i in range(MAX_YEAR):
    if i < MAX_SUPERVISED_YEAR:
        SUPERVISED_YEAR_MODIFIER.append(1)
    else:
        SUPERVISED_YEAR_MODIFIER.append(
            math.exp(
                -math.log(2) * (i - MAX_SUPERVISED_YEAR + 1) / HALF_SUPERVISED_YEAR
            )
        )

SUPERVISED_PAPER_MODIFIER = []

for i in range(MAX_PAPER):
    if i < MAX_SUPERVISED_PAPER:
        SUPERVISED_PAPER_MODIFIER.append(1)
    else:
        SUPERVISED_PAPER_MODIFIER.append(
            math.exp(
                -math.log(2) * (i - MAX_SUPERVISED_PAPER + 1) / HALF_SUPERVISED_PAPER
            )
        )


def compute_count_list(academic_year_list, paper_count_map, start_list=None):
    count_list = [0]
    for i in range(1, len(academic_year_list)):
        assert len(count_list) == i
        count = paper_count_map.get(academic_year_list[i - 1], 0)
        if start_list:
            count *= min(SUPERVISED_YEAR_MODIFIER[i - 1], SUPERVISED_PAPER_MODIFIER[int(start_list[i - 1])])
        count_list.append(count_list[-1] + count)
    return count_list


def compute_total_count(paper_count_map, year):
    academic_year_list = sorted(paper_count_map.keys())
    current_year_index = academic_year_list.index(year)
    total_count = 0
    for i in range(current_year_index):
        total_count += paper_count_map.get(academic_year_list[i], 0)
    return total_count


def compute_supervisor_rate(studentID, supervisorID, year):
    # if not studentID:
    #     cursorField.execute("select authorOrder from paper_author where paperID='%s' and authorID='%s'" % (paperID, supervisorID))
    #     return 1 / float(cursorField.fetchone()[0])
    # the sorted list of years that the student has paper publication, truncated to {0,1,..., MAX_ACADEMIC_YEAR}
    if studentID not in paperCountMap:
        print(studentID, 'not found in paperCountMap!')
        return 0.0
    student_academic_years = sorted(list(paperCountMap[studentID].keys()))[: MAX_ACADEMIC_YEAR + 1]
    if not (year in student_academic_years):
        return 0.0
    #
    yearIndex = student_academic_years.index(year)
    coAuthorID = f"{studentID}-{supervisorID}"
    faWeightedPaperCountMap = weightedPaperCountMap[studentID]
    try:
        caWeightedPaperCountMap = coWeightedPaperCountMap[coAuthorID]
    except Exception as e:
        print(coAuthorID, 'not found in caWeightedPaperCountMap!')
        return 0.0
    #
    start_student_count = compute_count_list(student_academic_years, faWeightedPaperCountMap)
    end_student_count = compute_count_list(student_academic_years[::-1], faWeightedPaperCountMap)[::-1]
    # the same as below except that co-author weighted count is replaced by student weighted count
    total_student_count = start_student_count[yearIndex] + end_student_count[yearIndex] \
        + faWeightedPaperCountMap[year];
    #
    start_coauthor_count = compute_count_list(student_academic_years, caWeightedPaperCountMap, start_student_count)
    end_coauthor_count = compute_count_list(student_academic_years[::-1], caWeightedPaperCountMap, start_student_count)[::-1]
    #
    total_coauthor_count = (
        start_coauthor_count[yearIndex] + end_coauthor_count[yearIndex]
        + caWeightedPaperCountMap[year]
        * min(SUPERVISED_YEAR_MODIFIER[yearIndex],
            SUPERVISED_PAPER_MODIFIER[int(start_student_count[yearIndex])],
        )
    )
    # iterate all possible year span (window) to compute the max supervisedRate
    maxSupervisedRate = 0.0
    maxStart, maxEnd = 0, 0
    for start_year_index in range(0, yearIndex + 1):
        for end_year_index in range(yearIndex, len(student_academic_years)):
            # there is a problem here: the co-authorship can happen in the same year,
            # because the surrounding years may not have co-authorship between student and supervisor
            # then the small window with year_span >= 2 can still be the maximal because the co-authorship
            # are too centralized in the same year
            #
            # we solve it by using a count list for co-authorship years
            #
            if (end_year_index - start_year_index + 1) < MIN_SUPERVISED_YEAR_SPAN:
                continue
            denominator =  total_student_count - start_student_count[start_year_index] - end_student_count[end_year_index]
            if denominator < MIN_SUPERVISED_PAPER_SPAN:
                continue
            numerator =  total_coauthor_count - start_coauthor_count[start_year_index] - end_coauthor_count[end_year_index]
            supervisedRate = numerator / denominator
            if supervisedRate > maxSupervisedRate:
                maxSupervisedRate = supervisedRate
                maxStart, maxEnd = start_year_index, end_year_index
    #
    maxSupervisedRate = min(1.0, maxSupervisedRate / MIN_SUPERVISED_RATE)
    # compute supervising rate
    total_supervisor_count = compute_total_count(paperCountMap[supervisorID], year)
    total_coauthor_count = compute_total_count(coPaperCountMap[coAuthorID], year)
    denominator = total_coauthor_count
    numerator = total_supervisor_count - total_coauthor_count
    #
    if numerator < 0:
        print(f"Error in computation, supervisor paper count smaller than co-author paper count: {studentID}, {supervisorID}")
        supervisingRate = 0.0
    elif numerator == 0:
        supervisingRate = 0.0
    elif denominator == 0:
        supervisingRate = MIN_SUPERVISING_RATE
    else:
        supervisingRate = numerator / denominator
    #
    supervisingRate = min(1.0, supervisingRate / MIN_SUPERVISING_RATE)
    # print(paperID, student_academic_years, maxSupervisedRate * supervisingRate)
    return maxSupervisedRate * supervisingRate



print("create util mapping", datetime.now().strftime("%H:%M:%S"))
authorID2name = df_authors.set_index('authorID')['name'].to_dict()
paperID2FirstAuthorID = df_paper_author[df_paper_author['authorOrder'] == 1].set_index('paperID')['authorID'].to_dict()

######################################################################
# 从数据库中查询某个领域的前几名作者。
# 对于每位作者，查询他们的论文信息。
# 对于每篇论文，确定它是否是一个“关键论文”（key paper）。
#       如果第一作者就是当前的顶级作者，则该论文被标记为关键论文。
#       否则，它会计算一个监督率（supervisor rate），并基于这个率来决定是否标记为关键论文。
# 更新数据库中的论文记录，标记它是否是关键论文。
# 提交数据库更改。
######################################################################
print('## start to process each author (key paper)', len(authorID_list), datetime.now().strftime("%H:%M:%S"))

def toStr(s):
    if type(s) == str:
        return s
    return s.iloc[0]

def build_top_author(pairs):
    authorID_list, order = pairs
    print(order, len(authorID_list))

    for authorID in tqdm(authorID_list):
        print('## ' + authorID)

        # Filter out rows from df_paper_author for the specific authorID
        df_paper_author_author = df_paper_author[df_paper_author['authorID'] == authorID]

        # Perform the same operations you did with SQL directly with pandas
        df = df_papers.merge(df_paper_author_author, on="paperID").groupby(['paperID', 'title', 'year']).agg({
            'authorOrder': 'min'
        }).reset_index()
        df['isKeyPaper'] = 0.0

        df['firstAuthorID'] = df['paperID'].map(paperID2FirstAuthorID)
        df['firstAuthorName'] = df['firstAuthorID'].map(authorID2name)

        # Process the DataFrame
        for i, row in df.iterrows():
            if pd.isna(row['firstAuthorID']):
                authorOrder = int(row['authorOrder'])
                print('Target paper does not have first author!', row['paperID'], authorOrder)
                isKeyPaper = 1 / authorOrder
            else:
                if row['firstAuthorID'] == authorID:
                    isKeyPaper = 1
                else:
                    isKeyPaper = compute_supervisor_rate(toStr(row['firstAuthorID']), authorID, int(row['year']))
            df.at[i, 'isKeyPaper'] = isKeyPaper
            print(row['paperID'], isKeyPaper)

        df.to_csv(f'out/{field}/papers_raw/{authorID}.csv', index=False)

multiprocess_num = multiprocessing.cpu_count()
with multiprocessing.Pool(processes=multiprocess_num) as pool:
    results = pool.map(build_top_author, [(authorID_list[i::multiprocess_num], f'{i}/{multiprocess_num}') for i in range(multiprocess_num)])

cursor.close()
conn.close()
