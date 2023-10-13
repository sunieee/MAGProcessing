import math
from utils import *
import json
import pandas as pd

connField = pymysql.connect(host='localhost',
                            port=3306,
                            user='root',
                            password='root',
                            db=f"{database}",
                            charset='utf8')
cursorField = connField.cursor()

def convert_keys(pairs):
    new_pairs = {}
    for key, value in pairs:
        if key.isdigit():
            new_pairs[int(key)] = value
        else:
            new_pairs[key] = value
    return new_pairs

# load all the maps to out/{database}/map/*.json with same name
with open(f"out/{database}/map/firstAuthorPaperCountMap.json", "r") as f:
    firstAuthorPaperCountMap = json.load(f, object_pairs_hook=convert_keys)
with open(f"out/{database}/map/firstAuthorWeightedPaperCountMap.json", "r") as f:
    firstAuthorWeightedPaperCountMap = json.load(f, object_pairs_hook=convert_keys)
with open(f"out/{database}/map/coAuthorWeightedPaperCountMap.json", "r") as f:
    coAuthorWeightedPaperCountMap = json.load(f, object_pairs_hook=convert_keys)
with open(f"out/{database}/map/coAuthorPaperCountMap.json", "r") as f:
    coAuthorPaperCountMap = json.load(f, object_pairs_hook=convert_keys)
with open(f"out/{database}/map/topAuthorPaperCountMap.json", "r") as f:
    topAuthorPaperCountMap = json.load(f, object_pairs_hook=convert_keys)

firstAuthorPaperCountMap = {str(k): v for k, v in firstAuthorPaperCountMap.items()}
firstAuthorWeightedPaperCountMap = {str(k): v for k, v in firstAuthorWeightedPaperCountMap.items()}
coAuthorWeightedPaperCountMap = {str(k): v for k, v in coAuthorWeightedPaperCountMap.items()}
coAuthorPaperCountMap = {str(k): v for k, v in coAuthorPaperCountMap.items()}
topAuthorPaperCountMap = {str(k): v for k, v in topAuthorPaperCountMap.items()}


MIN_STUDENT_AUTHOR_ORDER = 3
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


def computeSupervisorRate_old(studentID, supervisorID, year):
    # compute supervised rate
    studentPaperCountMap = firstAuthorPaperCountMap[studentID]
    # the sorted list of years that the student has paper publication, truncated to {0,1,..., MAX_ACADEMIC_YEAR}
    studentAcademicYearList = sorted(studentPaperCountMap.keys())[
        0 : MAX_ACADEMIC_YEAR + 1
    ]
    max_student_academic_year = len(studentAcademicYearList) - 1
    if not (year in studentAcademicYearList):
        return 0.0

    currentAcademicYearIndex = studentAcademicYearList.index(year)
    studentWeightedPaperCountMap = firstAuthorWeightedPaperCountMap[studentID]

    coAuthorID = studentID + "-" + supervisorID
    studentCoAuthorWeightedPaperCountMap = coAuthorWeightedPaperCountMap[coAuthorID]

    # the same as below except that co-author weighted count is replaced by student weighted count

    start_student_count_list = {}
    end_student_count_list = {}
    total_student_count = 0

    start_student_count_list[0] = 0
    for i in range(1, max_student_academic_year + 1):
        if studentAcademicYearList[i - 1] in studentWeightedPaperCountMap:
            start_student_count_list[i] = (
                start_student_count_list[i - 1]
                + studentWeightedPaperCountMap[studentAcademicYearList[i - 1]]
            )
        else:
            start_student_count_list[i] = start_student_count_list[i - 1]

    end_student_count_list[max_student_academic_year] = 0
    for i in range(max_student_academic_year - 1, currentAcademicYearIndex - 1, -1):
        if studentAcademicYearList[i + 1] in studentWeightedPaperCountMap:
            end_student_count_list[i] = (
                end_student_count_list[i + 1]
                + studentWeightedPaperCountMap[studentAcademicYearList[i + 1]]
            )
        else:
            end_student_count_list[i] = end_student_count_list[i + 1]

    total_student_count = (
        start_student_count_list[currentAcademicYearIndex]
        + end_student_count_list[currentAcademicYearIndex]
        + studentWeightedPaperCountMap[
            studentAcademicYearList[currentAcademicYearIndex]
        ]
    )

    # start_list[N] = accumulated weighted co-author paper count from academic year 0 to N-1, excluding the year N, N <= current_academic_year
    # end_list[N] = accumulated weighted co-author paper count from academic year N to MAX_ACADEMIC_YEAR, excluding the year N, N >= current_academic_year

    start_coauthor_count_list = {}
    end_coauthor_count_list = {}
    total_coauthor_count = 0

    start_coauthor_count_year_list = {}
    end_coauthor_count_year_list = {}
    total_coauthor_count_year = 0

    start_coauthor_count_list[0] = 0
    start_coauthor_count_year_list[0] = 0

    for i in range(1, currentAcademicYearIndex + 1):
        if studentAcademicYearList[i - 1] in studentCoAuthorWeightedPaperCountMap:
            start_coauthor_count_list[i] = start_coauthor_count_list[
                i - 1
            ] + studentCoAuthorWeightedPaperCountMap[
                studentAcademicYearList[i - 1]
            ] * min(
                SUPERVISED_YEAR_MODIFIER[i - 1],
                SUPERVISED_PAPER_MODIFIER[int(start_student_count_list[i - 1])],
            )
            start_coauthor_count_year_list[i] = (
                start_coauthor_count_year_list[i - 1] + 1
            )
        else:
            start_coauthor_count_list[i] = start_coauthor_count_list[i - 1]
            start_coauthor_count_year_list[i] = start_coauthor_count_year_list[i - 1]

    end_coauthor_count_list[max_student_academic_year] = 0
    end_coauthor_count_year_list[max_student_academic_year] = 0

    for i in range(max_student_academic_year - 1, currentAcademicYearIndex - 1, -1):
        if studentAcademicYearList[i + 1] in studentCoAuthorWeightedPaperCountMap:
            end_coauthor_count_list[i] = end_coauthor_count_list[
                i + 1
            ] + studentCoAuthorWeightedPaperCountMap[
                studentAcademicYearList[i + 1]
            ] * min(
                SUPERVISED_YEAR_MODIFIER[i + 1],
                SUPERVISED_PAPER_MODIFIER[int(start_student_count_list[i + 1])],
            )
            end_coauthor_count_year_list[i] = end_coauthor_count_year_list[i + 1] + 1
        else:
            end_coauthor_count_list[i] = end_coauthor_count_list[i + 1]
            end_coauthor_count_year_list[i] = end_coauthor_count_year_list[i + 1]

    total_coauthor_count = (
        start_coauthor_count_list[currentAcademicYearIndex]
        + end_coauthor_count_list[currentAcademicYearIndex]
        + studentCoAuthorWeightedPaperCountMap[
            studentAcademicYearList[currentAcademicYearIndex]
        ]
        * min(
            SUPERVISED_YEAR_MODIFIER[currentAcademicYearIndex],
            SUPERVISED_PAPER_MODIFIER[
                int(start_student_count_list[currentAcademicYearIndex])
            ],
        )
    )

    total_coauthor_count_year = (
        start_coauthor_count_year_list[currentAcademicYearIndex]
        + end_coauthor_count_year_list[currentAcademicYearIndex]
        + 1
    )

    # iterate all possible year span (window) to compute the max supervisedRate

    maxSupervisedRate = 0.0

    for start_year_index in range(0, currentAcademicYearIndex + 1):
        for end_year_index in range(
            currentAcademicYearIndex, max_student_academic_year + 1
        ):
            # there is a problem here: the co-authorship can happen in the same year,
            # because the surrounding years may not have co-authorship between student and supervisor
            # then the small window with year_span >= 2 can still be the maximal because the co-authorship
            # are too centralized in the same year
            #
            # we solve it by using a count list for co-authorship years
            #
            if (end_year_index - start_year_index + 1) < MIN_SUPERVISED_YEAR_SPAN:
                continue

            coauthor_count_year = (
                total_coauthor_count_year
                - start_coauthor_count_year_list[start_year_index]
                - end_coauthor_count_year_list[end_year_index]
            )

            if coauthor_count_year < MIN_SUPERVISED_YEAR_SPAN:
                continue

            denominator = (
                total_student_count
                - start_student_count_list[start_year_index]
                - end_student_count_list[end_year_index]
            )

            if denominator < MIN_SUPERVISED_PAPER_SPAN:
                continue

            numerator = (
                total_coauthor_count
                - start_coauthor_count_list[start_year_index]
                - end_coauthor_count_list[end_year_index]
            )

            supervisedRate = numerator / denominator

            if supervisedRate > maxSupervisedRate:
                maxSupervisedRate = supervisedRate

    maxSupervisedRate = min(1.0, maxSupervisedRate / MIN_SUPERVISED_RATE)

    # compute supervising rate
    supervisorPaperCountMap = topAuthorPaperCountMap[supervisorID]

    # the sorted list of years that the supervisor has paper publication
    supervisorAcademicYearList = sorted(supervisorPaperCountMap.keys())
    currentAcademicYearIndex = supervisorAcademicYearList.index(year)

    total_supervisor_count = 0
    for i in range(currentAcademicYearIndex):
        total_supervisor_count = (
            total_supervisor_count
            + supervisorPaperCountMap[supervisorAcademicYearList[i]]
        )

    coAuthorID = studentID + "-" + supervisorID
    studentCoAuthorPaperCountMap = coAuthorPaperCountMap[coAuthorID]

    coAuthorAcademicYearList = sorted(studentCoAuthorPaperCountMap.keys())
    currentAcademicYearIndex = coAuthorAcademicYearList.index(year)

    total_coauthor_count = 0
    for i in range(currentAcademicYearIndex):
        total_coauthor_count = (
            total_coauthor_count
            + studentCoAuthorPaperCountMap[coAuthorAcademicYearList[i]]
        )

    supervisingRate = 0.0

    denominator = total_coauthor_count
    numerator = total_supervisor_count - total_coauthor_count

    if numerator < 0:
        print(
            "Error in computation, supervisor paper count smaller than co-author paper count:",
            studentID,
            supervisorID,
        )
        supervisingRate = 0.0
    elif numerator == 0:
        supervisingRate = 0.0
    elif denominator == 0:
        supervisingRate = MIN_SUPERVISING_RATE
    else:
        supervisingRate = numerator / denominator

    supervisingRate = min(1.0, supervisingRate / MIN_SUPERVISING_RATE)

    return maxSupervisedRate * supervisingRate

######################################################################
# 从数据库中查询某个领域的前几名作者。
# 对于每位作者，查询他们的论文信息。
# 对于每篇论文，确定它是否是一个“关键论文”（key paper）。
#       如果第一作者就是当前的顶级作者，则该论文被标记为关键论文。
#       否则，它会计算一个监督率（supervisor rate），并基于这个率来决定是否标记为关键论文。
# 更新数据库中的论文记录，标记它是否是关键论文。
# 提交数据库更改。
######################################################################
print('## start to process each author (key paper)', len(authorID_list))

def build_top_author(pairs):
    authorID_list, order = pairs
    print(order, len(authorID_list))
    conn = pymysql.connect(host='localhost',
                            port=3306,
                            user='root',
                            password='root',
                            db=database_pcg,
                            charset='utf8')
    cursor = conn.cursor()

    for authorID in tqdm(authorID_list):
        print('###', authorID)
        paper_rows = executeFetch(f"select paperID, year, firstAuthorID, authorOrder from papers_{authorID}", cursor=cursor)
        # process each paper of the author
        print('start to process each paper of the author', len(paper_rows))
        for paper_row in paper_rows:
            paperID = str(paper_row[0].strip())
            paperYear = int(paper_row[1])
            if not paper_row[2]:
                authorOrder = int(paper_row[3])
                print('Target paper do not have first author!', paperID, authorOrder)
                isKeyPaper = 1 / authorOrder
            else:
                firstAuthorID = str(paper_row[2].strip())
                if firstAuthorID == authorID:
                    isKeyPaper = 1
                else:
                    isKeyPaper = computeSupervisorRate_old(firstAuthorID, authorID, paperYear)
            print(paperID, isKeyPaper)

            cursor.execute(f"update papers_{authorID} set isKeyPaper = %s where paperID = %s", (isKeyPaper, paperID))

        # print(f"Update key papers for field author {authorName} with rank {rank}: authorID",)
        conn.commit()

        papers_df = pd.read_sql(f"SELECT * FROM papers_{authorID}", conn)
        papers_df.to_csv(f'out/{database}/papers/{authorID}.csv', index=False)

    cursor.close()
    conn.close()


with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(build_top_author, [(authorID_list[i::multiproces_num], f'{i}/{multiproces_num}') for i in range(multiproces_num)])

# dump all top field authors
df = pd.read_sql(f"""select * from {database}.authors_field
    where {filterCondition}""", conn)
df.to_csv(f'out/{database}/top_field_authors.csv', index=False)

cursor.close()
cursorField.close()
conn.close()
connField.close()