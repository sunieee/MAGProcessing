from utils import *
from tqdm import tqdm


#######################################################################
# update authors_field
# 计算并添加作者在领域内的论文数量及排名，更新作者的引用总数信息，添加 h-index 信息，创建名为 paper_reference_field_labeled 的表，添加名为 FellowType 列
#######################################################################
print('updating authors_field')

try_execute("drop table paper_reference_field_labeled;")
try_execute("drop table authors_field_tmp;")
try_execute("ALTER TABLE authors_field DROP COLUMN PaperCount_field;")
try_execute("ALTER TABLE authors_field DROP COLUMN authorRank;")
try_execute("ALTER TABLE authors_field DROP COLUMN CitationCount_field;")
try_execute("ALTER TABLE authors_field DROP COLUMN hIndex_field;")
try_execute("ALTER TABLE authors_field DROP COLUMN FellowType;")


execute('''
create table authors_field_tmp select tmp.*, @curRank := @curRank + 1 AS authorRank 
    from (select authorID, count(*) as PaperCount_field 
        from paper_author_field 
        group by authorID 
        order by PaperCount_field desc) 
    as tmp, (SELECT @curRank := 0) r;
        
create index author_index on authors_field_tmp(authorID);
ALTER TABLE authors_field ADD PaperCount_field INT;
ALTER TABLE authors_field ADD authorRank INT;
update authors_field, authors_field_tmp 
    set authors_field.PaperCount_field = authors_field_tmp.PaperCount_field, 
        authors_field.authorRank = authors_field_tmp.authorRank 
        where authors_field.authorID = authors_field_tmp.authorID;

alter table authors_field add index(authorRank);
drop table authors_field_tmp;

create table authors_field_tmp 
    select sum(P.citationCount) as CitationCount_field,  authorID 
    from papers_field as P join paper_author_field as PA on P.paperID = PA.paperID and P.CitationCount >=0 group by authorID;
create index id_index on authors_field_tmp(authorID);
ALTER TABLE authors_field ADD CitationCount_field INT;
update authors_field, authors_field_tmp 
    set authors_field.CitationCount_field = authors_field_tmp.CitationCount_field 
    where authors_field.authorID = authors_field_tmp.authorID;
drop table authors_field_tmp;

ALTER TABLE authors_field ADD hIndex_field INT;
create table paper_reference_field_labeled(
citingpaperID varchar(15),
citedpaperID varchar(15),
extends_prob double
);
''')

# ALTER TABLE authors_field ADD FellowType varchar(999);
# update authors_field as af, scigene_acl_anthology.fellow as f 
#     set af.FellowType='1' where af.name = f.name and af.authorRank<=1000 and f.type=1 and CitationCount_field>=1000



#######################################################################
# calculate hIndex
# 通过计算每位作者的引用次数数据，根据 h-index 的定义，计算并更新了每位作者在特定领域内的 h-index 值
# 以反映其影响力和论文引用分布情况。
#######################################################################
# numOfTopAuthors = 1100
# # filterCondition = f'authorRank <= {numOfTopAuthors}';
# filterCondition = f'PaperCount_field > 10';

# # select all top field authors
# cursor.execute(f"select authorID, name, PaperCount_field, authorRank from {database}.authors_field where {filterCondition};")
# rows = cursor.fetchall()

# # process each author
# for row in tqdm(rows):
#     authorID = row[0].strip()
#     cursor.execute("""select P.CitationCount from papers_field as P 
#                    join paper_author_field as PA 
#                    on PA.authorID = %s and P.paperID = PA.paperID;""", authorID)
#     citations = [int(citation_row[0]) for citation_row in cursor.fetchall()]
#     citations.sort(reverse=True)
#     hIndex_field = sum(1 for i, citation in enumerate(citations) if citation > i)

#     cursor.execute(
#         "update authors_field set hIndex_field = %s where authorID = %s",
#         (hIndex_field, authorID)
#     )
#     conn.commit()
#     # print("Process author: ", authorName, " with rank ", str(authorRank))

# cursor.close()
# conn.close()