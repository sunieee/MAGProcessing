import math
from utils import *
import json
import pandas as pd
import datetime

def convert_keys(pairs):
    new_pairs = {}
    for key, value in pairs:
        if key.isdigit():
            new_pairs[int(key)] = value
        else:
            new_pairs[key] = value
    return new_pairs

# load all the maps to {path_to_mapping}/*.json with same name
print("load all the maps!", datetime.datetime.now().strftime("%H:%M:%S"))
path_to_mapping = f'out/{database}/map'
with open(f"{path_to_mapping}/firstAuthorPaperCountMap.json", "r") as f:
    firstAuthorPaperCountMap = json.load(f, object_pairs_hook=convert_keys)
with open(f"{path_to_mapping}/firstAuthorWeightedPaperCountMap.json", "r") as f:
    firstAuthorWeightedPaperCountMap = json.load(f, object_pairs_hook=convert_keys)
with open(f"{path_to_mapping}/coAuthorWeightedPaperCountMap.json", "r") as f:
    coAuthorWeightedPaperCountMap = json.load(f, object_pairs_hook=convert_keys)
with open(f"{path_to_mapping}/coAuthorPaperCountMap.json", "r") as f:
    coAuthorPaperCountMap = json.load(f, object_pairs_hook=convert_keys)
with open(f"{path_to_mapping}/topAuthorPaperCountMap.json", "r") as f:
    topAuthorPaperCountMap = json.load(f, object_pairs_hook=convert_keys)

firstAuthorPaperCountMap = {str(k): v for k, v in firstAuthorPaperCountMap.items()}
firstAuthorWeightedPaperCountMap = {str(k): v for k, v in firstAuthorWeightedPaperCountMap.items()}
coAuthorWeightedPaperCountMap = {str(k): v for k, v in coAuthorWeightedPaperCountMap.items()}
coAuthorPaperCountMap = {str(k): v for k, v in coAuthorPaperCountMap.items()}
topAuthorPaperCountMap = {str(k): v for k, v in topAuthorPaperCountMap.items()}


MIN_SUPERVISOR_RATE = 0.5
MIN_SUPERVISED_RATE = 0.6
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
    #     cursorField.execute("select authorOrder from paper_author_field where paperID='%s' and authorID='%s'" % (paperID, supervisorID))
    #     return 1 / float(cursorField.fetchone()[0])

    # the sorted list of years that the student has paper publication, truncated to {0,1,..., MAX_ACADEMIC_YEAR}
    student_academic_years = sorted(list(firstAuthorPaperCountMap[studentID].keys()))[: MAX_ACADEMIC_YEAR + 1]
    if not (year in student_academic_years):
        return 0.0
    
    yearIndex = student_academic_years.index(year)
    coAuthorID = f"{studentID}-{supervisorID}"
    faWeightedPaperCountMap = firstAuthorWeightedPaperCountMap[studentID]
    caWeightedPaperCountMap = coAuthorWeightedPaperCountMap[coAuthorID]
    
    start_student_count = compute_count_list(student_academic_years, faWeightedPaperCountMap)
    end_student_count = compute_count_list(student_academic_years[::-1], faWeightedPaperCountMap)[::-1]

    # the same as below except that co-author weighted count is replaced by student weighted count
    total_student_count = start_student_count[yearIndex] + end_student_count[yearIndex] \
        + faWeightedPaperCountMap[year];

    start_coauthor_count = compute_count_list(student_academic_years, caWeightedPaperCountMap, start_student_count)
    end_coauthor_count = compute_count_list(student_academic_years[::-1], caWeightedPaperCountMap, start_student_count)[::-1]

    total_coauthor_count = (
        start_coauthor_count[yearIndex] + end_coauthor_count[yearIndex]
        + caWeightedPaperCountMap[year]
        * min(SUPERVISED_YEAR_MODIFIER[yearIndex],
            SUPERVISED_PAPER_MODIFIER[int(start_student_count[yearIndex])],
        )
    )

    # iterate all possible year span (window) to compute the max supervisedRate
    maxSupervisedRate = 0.0

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

            maxSupervisedRate = max(maxSupervisedRate, supervisedRate)

    maxSupervisedRate = min(1.0, maxSupervisedRate / MIN_SUPERVISED_RATE)

    # compute supervising rate
    total_supervisor_count = compute_total_count(topAuthorPaperCountMap[supervisorID], year)
    total_coauthor_count = compute_total_count(coAuthorPaperCountMap[coAuthorID], year)
    
    denominator = total_coauthor_count
    numerator = total_supervisor_count - total_coauthor_count

    if numerator < 0:
        print(f"Error in computation, supervisor paper count smaller than co-author paper count: {studentID}, {supervisorID}")
        supervisingRate = 0.0
    elif numerator == 0:
        supervisingRate = 0.0
    elif denominator == 0:
        supervisingRate = MIN_SUPERVISING_RATE
    else:
        supervisingRate = numerator / denominator

    supervisingRate = min(1.0, supervisingRate / MIN_SUPERVISING_RATE)
    # print(paperID, student_academic_years, maxSupervisedRate * supervisingRate)
    return maxSupervisedRate * supervisingRate



print("create util mapping", datetime.datetime.now().strftime("%H:%M:%S"))
authorID2name = df_authors_field.set_index('authorID')['name'].to_dict()
paperID2FirstAuthorID = df_paper_author_field[df_paper_author_field['authorOrder'] == 1].set_index('paperID')['authorID'].to_dict()

######################################################################
# 从数据库中查询某个领域的前几名作者。
# 对于每位作者，查询他们的论文信息。
# 对于每篇论文，确定它是否是一个“关键论文”（key paper）。
#       如果第一作者就是当前的顶级作者，则该论文被标记为关键论文。
#       否则，它会计算一个监督率（supervisor rate），并基于这个率来决定是否标记为关键论文。
# 更新数据库中的论文记录，标记它是否是关键论文。
# 提交数据库更改。
######################################################################
print('## start to process each author (key paper)', len(authorID_list), datetime.datetime.now().strftime("%H:%M:%S"))

def build_top_author(pairs):
    authorID_list, order = pairs
    print(order, len(authorID_list))

    for authorID in tqdm(authorID_list):
        print('## ' + authorID)

        # Filter out rows from df_paper_author_field for the specific authorID
        df_paper_author_field_author = df_paper_author_field[df_paper_author_field['authorID'] == authorID]

        # Perform the same operations you did with SQL directly with pandas
        df = df_papers_field.merge(df_paper_author_field_author, on="paperID").groupby(['paperID', 'title', 'year']).agg({
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
                    isKeyPaper = compute_supervisor_rate(row['firstAuthorID'], authorID, int(row['year']))
            df.at[i, 'isKeyPaper'] = isKeyPaper
            print(row['paperID'], isKeyPaper)

        df.to_csv(f'out/{database}/papers/{authorID}.csv', index=False)


with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(build_top_author, [(authorID_list[i::multiproces_num], f'{i}/{multiproces_num}') for i in range(multiproces_num)])

cursor.close()
conn.close()