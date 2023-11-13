from utils import database, execute, try_execute, create_connection, cursor, conn
import pandas as pd
import math
import multiprocessing
from tqdm import tqdm

####################################################################################
# update papers_field
# 将论文表中的年份更新为发布日期的年份，为年份添加索引，然后从papers_field_citation_timeseries表复制引用次数序列数据到新的列中。
####################################################################################
print('updating papers_field')
try_execute("ALTER TABLE papers_field DROP COLUMN citationCountByYear;")

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


#######################################################################
# update authors_field
# 计算并添加作者在领域内的论文及引用数量，更新作者的引用总数信息
# 通过计算每位作者的引用次数数据，根据 h-index 的定义，计算并更新了每位作者在特定领域内的 h-index 值
#######################################################################
print('updating authors_field')

try_execute("ALTER TABLE authors_field DROP COLUMN PaperCount_field;")
try_execute("ALTER TABLE authors_field DROP COLUMN CitationCount_field;")
try_execute("ALTER TABLE authors_field DROP COLUMN hIndex_field;")
try_execute("ALTER TABLE authors_field DROP COLUMN FellowType;")

execute('''
ALTER TABLE authors_field ADD PaperCount_field INT DEFAULT 0;
UPDATE authors_field af
JOIN (
    SELECT authorID, COUNT(*) as count_papers
    FROM paper_author_field
    GROUP BY authorID
) tmp ON af.authorID = tmp.authorID
SET af.PaperCount_field = tmp.count_papers;

ALTER TABLE authors_field ADD CitationCount_field INT DEFAULT 0;
UPDATE authors_field af
JOIN (
    SELECT PA.authorID, SUM(P.citationCount) as total_citations
    FROM papers_field as P 
    JOIN paper_author_field as PA on P.paperID = PA.paperID 
    WHERE P.CitationCount >= 0 
    GROUP BY PA.authorID
) tmp ON af.authorID = tmp.authorID
SET af.CitationCount_field = tmp.total_citations;

ALTER TABLE authors_field ADD hIndex_field INT DEFAULT 0;
''')

# 过滤掉不重要的人（剩下约1/8的人），节省计算时间
# 1532927 -> 273005(1) -> 120834(2) -> 71839(3)
authorID_list = pd.read_sql(f"SELECT authorID FROM authors_field where PaperCount_field>2", conn)['authorID'].tolist()

def extract_hIndex(pair):
    authorID_list, order = pair
    print(f'* extracting hIndex: {order}')
    conn, cursor = create_connection(database)
    for authorID in tqdm(authorID_list):
        cursor.execute(f"""select P.CitationCount from papers_field as P 
                    join paper_author_field as PA 
                    on PA.authorID = '{authorID}' and P.paperID = PA.paperID;""")
        rows = cursor.fetchall()
        citations = [int(citation_row[0]) for citation_row in rows]
        citations.sort(reverse=True)
        hIndex_field = sum(1 for i, citation in enumerate(citations) if citation > i)

        cursor.execute(
            "update authors_field set hIndex_field = %s where authorID = %s",
            (hIndex_field, authorID)
        )
        conn.commit()
    cursor.close()
    conn.close()


multiproces_num = 20
group_size = 2000
group_length = math.ceil(len(authorID_list)/group_size)
with multiprocessing.Pool(processes=multiproces_num * 5) as pool:
    results = pool.map(extract_hIndex, [(authorID_list[i*group_size:(i+1)*group_size], f'{i}/{group_length}') for i in range(group_length)])


# ALTER TABLE authors_field ADD FellowType varchar(999);
# update authors_field as af, scigene_acl_anthology.fellow as f 
#     set af.FellowType='1' where af.name = f.name and af.authorRank<=1000 and f.type=1 and CitationCount_field>=1000


cursor.close()
conn.close()