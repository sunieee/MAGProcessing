import math
from utils import *

connField = pymysql.connect(host='localhost',
                            port=3306,
                            user='root',
                            password='root',
                            db=f"scigene_{fieldName}_field",
                            charset='utf8')
cursorField = connField.cursor()

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


# pre-compute some maps
######################################################################
# 从数据库中查询某个领域的论文作者信息，并基于查询结果构建两个映射：
# 1. firstAuthorPaperCountMap：映射每个作者ID到一个子映射，其中子映射的键是年份，值是该年份的论文数量。
# 2. firstAuthorWeightedPaperCountMap：与上面的映射类似，但值是加权的论文数量，
#       其中权重是1/作者顺序，但仅考虑作者顺序小于或等于MIN_STUDENT_AUTHOR_ORDER的作者。
######################################################################

try:
    execute(f"""
create table firstAuthorTmp 
    select PA2.authorID as firstAuthorID, PA1.authorID as topAuthorID 
    from scigene_{fieldName}_field.authors_field as A 
    join scigene_{fieldName}_field.paper_author_field as PA1 on A.authorID = PA1.authorID 
    join scigene_{fieldName}_field.paper_author_field as PA2 on PA1.paperID = PA2.paperID 
    where A.{filterCondition} 
        and PA1.authorOrder > 1 and PA2.authorOrder = 1 
    group by PA2.authorID, PA1.authorID;
            
create index first_author_index on firstAuthorTmp(firstAuthorID);
            
create index top_author_index on firstAuthorTmp(topAuthorID);""")
except Exception as e:
    print("FirstAuthorTmp exists", e)

print("Create temp table for the list of first authors!")

rows = executeFetch(f"""
select authorID, authorOrder, year, count(*) as cnt 
    from scigene_{fieldName}_field.paper_author_field as PA, 
        scigene_{fieldName}_field.papers_field as P 
    where authorID in (select distinct firstAuthorID from firstAuthorTmp) and PA.paperID = P.paperID 
    group by authorID, authorOrder, year;""")
firstAuthorPaperCountMap = {}
firstAuthorWeightedPaperCountMap = {}

for authorID, authorOrder, year, count in rows:
    authorID = authorID.strip()
    authorOrder = int(authorOrder)
    year = int(year)
    count = int(count)

    # 更新firstAuthorPaperCountMap
    yearCountMap = firstAuthorPaperCountMap.setdefault(authorID, {})
    yearCountMap[year] = yearCountMap.get(year, 0) + count

    # 更新firstAuthorWeightedPaperCountMap
    if authorOrder <= MIN_STUDENT_AUTHOR_ORDER:
        yearWeightedCountMap = firstAuthorWeightedPaperCountMap.setdefault(authorID, {})
        yearWeightedCountMap[year] = yearWeightedCountMap.get(year, 0) + count / authorOrder


######################################################################
# 从数据库中查询某个领域的论文合作者信息，并基于查询结果构建两个映射：
# 1. coAuthorWeightedPaperCountMap：映射每个合作者ID对（由两个作者ID组成）到一个子映射，
#   其中子映射的键是年份，值是加权的论文数量，其中权重是1/作者顺序。
# 2. coAuthorPaperCountMap：与上面的映射类似，但值是该年份的论文数量。
######################################################################
print("Pre-compute first-author maps!")

rows = executeFetch(f"""
select firstAuthorID, topAuthorID, PA1.authorOrder as firstAuthorOrder, year, count(*) as cnt 
    from firstAuthorTmp 
    join scigene_{fieldName}_field.paper_author_field as PA1 on firstAuthorID = PA1.authorID 
    join scigene_{fieldName}_field.paper_author_field as PA2 on topAuthorID = PA2.authorID 
        and PA1.paperID = PA2.paperID and PA1.authorOrder <= {MIN_STUDENT_AUTHOR_ORDER} 
        and PA1.authorOrder < PA2.authorOrder 
    join scigene_{fieldName}_field.papers_field as P on PA1.paperID = P.paperID 
    group by firstAuthorID, topAuthorID, PA1.authorOrder, year;
""")
coAuthorWeightedPaperCountMap = {}
coAuthorPaperCountMap = {}

# 处理查询结果
for firstAuthorID, topAuthorID, authorOrder, year, count in rows:
    coAuthorID = f"{firstAuthorID.strip()}-{topAuthorID.strip()}"
    authorOrder = int(authorOrder)
    year = int(year)
    count = int(count)

    # 更新coAuthorWeightedPaperCountMap
    yearWeightedCountMap = coAuthorWeightedPaperCountMap.setdefault(coAuthorID, {})
    yearWeightedCountMap[year] = yearWeightedCountMap.get(year, 0) + count / authorOrder

    # 更新coAuthorPaperCountMap
    yearCountMap = coAuthorPaperCountMap.setdefault(coAuthorID, {})
    yearCountMap[year] = yearCountMap.get(year, 0) + count


######################################################################
# 这段代码的目的是处理查询结果，并基于这些结果构建一个映射topAuthorPaperCountMap。
# 这个映射的键是作者ID，值是另一个映射，其中子映射的键是年份，值是该年份的论文数量。
######################################################################
print("Pre-compute co-author maps!")

rows = executeFetch(f"""
select A.authorID, year, count(*) as cnt 
    from scigene_{fieldName}_field.authors_field as A 
    join scigene_{fieldName}_field.paper_author_field as PA 
        on A.{filterCondition}
        and A.authorID = PA.authorID  
    join scigene_{fieldName}_field.papers_field as P on PA.paperID = P.paperID 
    group by A.authorID, year;""")

topAuthorPaperCountMap = {}

for authorID, year, count in rows:
    authorID = authorID.strip()
    year = int(year)
    count = int(count)

    # 更新topAuthorPaperCountMap
    yearCountMap = topAuthorPaperCountMap.setdefault(authorID, {})
    yearCountMap[year] = yearCountMap.get(year, 0) + count



######################################################################
# 从数据库中查询某个领域的前几名作者。
# 对于每位作者，查询他们的论文信息。
# 对于每篇论文，确定它是否是一个“关键论文”（key paper）。
#       如果第一作者就是当前的顶级作者，则该论文被标记为关键论文。
#       否则，它会计算一个监督率（supervisor rate），并基于这个率来决定是否标记为关键论文。
# 更新数据库中的论文记录，标记它是否是关键论文。
# 提交数据库更改。
######################################################################
print("Pre-compute top author maps!")
rows = executeFetch(f"""
select authorID, name, authorRank 
    from scigene_{fieldName}_field.authors_field 
    where {filterCondition};""")

# process each author
for topAuthorID, authorName, rank in rows:
    topAuthorID = topAuthorID.strip()
    authorName = authorName.strip()
    rank = int(rank)

    authorTableName = "".join(filter(str.isalpha, authorName)).lower() + str(rank)
    paper_rows = executeFetch(f"select paperID, year, firstAuthorID from papers_{authorTableName}")

    print('='*20, authorName, '='*20)
    # process each paper of the author
    for paper_row in paper_rows:
        paperID = str(paper_row[0].strip())
        paperYear = int(paper_row[1])
        if not paper_row[2]:
            print('Target paper do not have first author, skip!', paperID)
            continue
        firstAuthorID = str(paper_row[2].strip())

        if firstAuthorID == topAuthorID:
            isKeyPaper = 1
        else:
            # try:
            isKeyPaper = compute_supervisor_rate(firstAuthorID, topAuthorID, paperYear, paperID)
            # except Exception as e:
            #     print('The row is not valid:')
            #     print('firstAuthorID:', firstAuthorID)
            #     print('topAuthorID:', topAuthorID)
            #     print('paperYear:', paperYear)
            #     print('paperID:', paperID)
            #     print(e)
            #     exit(0)

        cursor.execute(f"update papers_{authorTableName} set isKeyPaper = %s where paperID = %s", (isKeyPaper, paperID))

    print(f"Update key papers for field author {authorName} with rank {rank}: authorTableName",)
    conn.commit()

cursor.execute("drop table firstAuthorTmp;")
conn.commit()


cursor.close()
cursorField.close()
conn.close()
connField.close()