import math
from utils import *
import json

connField = pymysql.connect(host='localhost',
                            port=3306,
                            user='root',
                            password='root',
                            db=f"{database}",
                            charset='utf8')
cursorField = connField.cursor()

# load all the maps to data/{database}/map/*.json with same name
with open(f"data/{database}/map/firstAuthorPaperCountMap.json", "r") as f:
    firstAuthorPaperCountMap = json.load(f)
with open(f"data/{database}/map/firstAuthorWeightedPaperCountMap.json", "r") as f:
    firstAuthorWeightedPaperCountMap = json.load(f)
with open(f"data/{database}/map/coAuthorWeightedPaperCountMap.json", "r") as f:
    coAuthorWeightedPaperCountMap = json.load(f)
with open(f"data/{database}/map/coAuthorPaperCountMap.json", "r") as f:
    coAuthorPaperCountMap = json.load(f)
with open(f"data/{database}/map/topAuthorPaperCountMap.json", "r") as f:
    topAuthorPaperCountMap = json.load(f)

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


def compute_supervisor_rate(studentID, supervisorID, year, paperID):
    if not studentID:
        cursorField.execute("select authorOrder from paper_author_field where paperID='%s' and authorID='%s'" % (paperID, supervisorID))
        return 1 / float(cursorField.fetchone()[0])

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


######################################################################
# 从数据库中查询某个领域的前几名作者。
# 对于每位作者，查询他们的论文信息。
# 对于每篇论文，确定它是否是一个“关键论文”（key paper）。
#       如果第一作者就是当前的顶级作者，则该论文被标记为关键论文。
#       否则，它会计算一个监督率（supervisor rate），并基于这个率来决定是否标记为关键论文。
# 更新数据库中的论文记录，标记它是否是关键论文。
# 提交数据库更改。
######################################################################
print('## start to process each author (key paper)', len(authors_rows))
count = 0
for row in tqdm(authors_rows):
    authorID = row[0].strip()
    authorName = row[1].strip()
    rank = int(row[2])
    count += 1
    print(f'### ({count}/{len(authors_rows)})', authorID, authorName, rank)

    # authorID = "".join(filter(str.isalpha, authorName)).lower() + str(rank)
    paper_rows = executeFetch(f"select paperID, year, firstAuthorID from papers_{authorID}")

    print('='*20, authorName, '='*20)
    # process each paper of the author
    for paper_row in paper_rows:
        paperID = str(paper_row[0].strip())
        paperYear = int(paper_row[1])
        if not paper_row[2]:
            # firstAuthorID = executeFetch(f"select authorID from MACG.paper_author where paperID='{paperID}' and authorOrder=1")
            # firstAuthorID = firstAuthorID[0][0]
            # print(firstAuthorID)    # [('3137927773',)] -> '3137927773'
            # execute(f"update papers_{authorID} set firstAuthorID='{firstAuthorID}' where paperID='{paperID}';")
            # print('update firstAuthorID:', firstAuthorID, 'for paperID:', paperID)
            print('Target paper do not have first author, skip!', paperID)
            continue

        firstAuthorID = str(paper_row[2].strip())

        if firstAuthorID == authorID:
            isKeyPaper = 1
        else:
            # try:
            isKeyPaper = compute_supervisor_rate(firstAuthorID, authorID, paperYear, paperID)
            # except Exception as e:
            #     print('The row is not valid:')
            #     print('firstAuthorID:', firstAuthorID)
            #     print('topAuthorID:', topAuthorID)
            #     print('paperYear:', paperYear)
            #     print('paperID:', paperID)
            #     print(e)
            #     exit(0)

        cursor.execute(f"update papers_{authorID} set isKeyPaper = %s where paperID = %s", (isKeyPaper, paperID))

    print(f"Update key papers for field author {authorName} with rank {rank}: authorID",)
    conn.commit()


cursor.close()
cursorField.close()
conn.close()
connField.close()