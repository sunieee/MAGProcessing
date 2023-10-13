from utils import *
import json

# pre-compute some maps
######################################################################
# 从数据库中查询某个领域的论文作者信息，并基于查询结果构建两个映射：
# 1. firstAuthorPaperCountMap：映射每个作者ID到一个子映射，其中子映射的键是年份，值是该年份的论文数量。
# 2. firstAuthorWeightedPaperCountMap：与上面的映射类似，但值是加权的论文数量，
#       其中权重是1/作者顺序，但仅考虑作者顺序小于或等于MIN_STUDENT_AUTHOR_ORDER的作者。
######################################################################
MIN_STUDENT_AUTHOR_ORDER = 3

try_execute(f"drop table firstAuthorTmp;")

execute(f"""
create table firstAuthorTmp 
    select PA2.authorID as firstAuthorID, PA1.authorID as topAuthorID 
    from {database}.authors_field as A 
    join {database}.paper_author_field as PA1 on A.authorID = PA1.authorID 
    join {database}.paper_author_field as PA2 on PA1.paperID = PA2.paperID 
    where A.{filterCondition} 
        and PA1.authorOrder > 1 and PA2.authorOrder = 1 
    group by PA2.authorID, PA1.authorID;
            
create index first_author_index on firstAuthorTmp(firstAuthorID);
            
create index top_author_index on firstAuthorTmp(topAuthorID);""")

print("Create temp table for the list of first authors!")
rows = executeFetch(f"""
select authorID, authorOrder, year, count(*) as cnt 
    from {database}.paper_author_field as PA, 
        {database}.papers_field as P 
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
    join {database}.paper_author_field as PA1 on firstAuthorID = PA1.authorID 
    join {database}.paper_author_field as PA2 on topAuthorID = PA2.authorID 
        and PA1.paperID = PA2.paperID and PA1.authorOrder <= {MIN_STUDENT_AUTHOR_ORDER} 
        and PA1.authorOrder < PA2.authorOrder 
    join {database}.papers_field as P on PA1.paperID = P.paperID 
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
    from {database}.authors_field as A 
    join {database}.paper_author_field as PA 
        on A.{filterCondition}
        and A.authorID = PA.authorID  
    join {database}.papers_field as P on PA.paperID = P.paperID 
    group by A.authorID, year;""")

topAuthorPaperCountMap = {}

for authorID, year, count in rows:
    authorID = authorID.strip()
    year = int(year)
    count = int(count)

    # 更新topAuthorPaperCountMap
    yearCountMap = topAuthorPaperCountMap.setdefault(authorID, {})
    yearCountMap[year] = yearCountMap.get(year, 0) + count


# save all the maps to out/{database}/map/*.json
with open(f"out/{database}/map/firstAuthorPaperCountMap.json", "w") as f:
    json.dump(firstAuthorPaperCountMap, f)
with open(f"out/{database}/map/firstAuthorWeightedPaperCountMap.json", "w") as f:
    json.dump(firstAuthorWeightedPaperCountMap, f)
with open(f"out/{database}/map/coAuthorWeightedPaperCountMap.json", "w") as f:
    json.dump(coAuthorWeightedPaperCountMap, f)
with open(f"out/{database}/map/coAuthorPaperCountMap.json", "w") as f:
    json.dump(coAuthorPaperCountMap, f)
with open(f"out/{database}/map/topAuthorPaperCountMap.json", "w") as f:
    json.dump(topAuthorPaperCountMap, f)
