# 该代码isKeyPaper仍为0
from utils import *

# 选取前NUMTOPAUTHORS个学者
cursor.execute(f"""select authorID, name, authorRank 
               from scigene_{fieldName}_field.authors_field 
               where {filterCondition}
""")

rows = cursor.fetchall()

# process each author
for topAuthorID, authorName, rank in rows:
    topAuthorID = topAuthorID.strip()
    authorName = authorName.strip()
    rank = int(rank)

    authorTableName = "".join(filter(str.isalpha, authorName)).lower() + str(rank)

    # drop arc author db
    dropfieldPapers_author = f"drop table papers_{authorTableName}"
    try:
        cursor.execute(dropfieldPapers_author)
        conn.commit()
    except Exception as e:
        print("No such table:", dropfieldPapers_author)
    
    #########################################################################
    # 创建表papers_xiaofengli1：paperID, title, year, referenceCount, citationCount, authorOrder, isKeyPaper, firstAuthorID, firstAuthorName
    # 表paper_author_field中authorOrder = 1的authorID为firstAuthorID
    # 更新firstAuthorName
    #########################################################################
    execute(f"""
create table papers_{authorTableName} (firstAuthorID varchar(15), firstAuthorName varchar(999), isKeyPaper float) 
    select papers_field.paperID, title, year, referenceCount, citationCount, min(authorOrder) as authorOrder, 
    0 as isKeyPaper, '' as firstAuthorID, '' as firstAuthorName from scigene_{fieldName}_field.paper_author_field, 
    scigene_{fieldName}_field.papers_field where authorID = ? and papers_field.paperID = paper_author_field.paperID 
    group by papers_field.paperID, title, year;
            
create index arc_index on papers_{authorTableName}(paperID);

update papers_{authorTableName} as P, scigene_{fieldName}_field.paper_author_field as PA 
    set P.firstAuthorID = PA.authorID where P.paperID = PA.paperID and PA.authorOrder = 1;

update papers_{authorTableName} as P, scigene_{fieldName}_field.authors_field as A 
    set P.firstAuthorName = A.name where P.firstAuthorID = A.authorID;

select * from papers_{authorTableName} INTO OUTFILE 'data/csv/{fieldName}/papers_{authorTableName}.csv' 
    FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n';
""")

    print(f"Create and fill papers for field author {authorName}  with rank {rank}: {authorTableName}")
    dropfieldLinks_author = f"drop table links_{authorTableName}"
    try:
        cursor.execute(dropfieldLinks_author)
        conn.commit()
    except Exception as e:
        print("No such table:", dropfieldLinks_author)

    #########################################################################
    # insert potential influence link table for the author
    # 创建links_xiaofengli1：citingpaperID, citedpaperID, sharedAuthor, extends_prob。其中citingpaperID均来自papers_xiaofengli1
    # citingpaperID和citedpaperID均属于xiaofengli1，则sharedAuthor=1
    # 从paper_reference_field_labeled更新extends_prob
    #########################################################################
    execute(f"""
create table links_{authorTableName} (extends_prob float) 
    select P.citingpaperID, P.citedpaperID, 0 as sharedAuthor, null as extends_prob 
    from scigene_{fieldName}_field.paper_reference_field as P 
    where P.citingpaperID in (select paperID from papers_{authorTableName}) 
    group by P.citingpaperID, P.citedpaperID;

create index citing_index on links_{authorTableName}(citingpaperID);
create index cited_index on links_{authorTableName}(citedpaperID);

update links_{authorTableName} as P, scigene_{fieldName}_field.paper_author_field as A, 
    scigene_{fieldName}_field.paper_author_field as B set P.sharedAuthor = 1 
    where A.paperID = P.citingpaperID and B.paperID = P.citedpaperID and A.authorID = B.authorID";

update links_{authorTableName}, scigene_{fieldName}_field.paper_reference_field_labeled 
    set links_{authorTableName}.extends_prob = paper_reference_field_labeled.extends_prob 
    where links_{authorTableName}.citingpaperID = paper_reference_field_labeled.citingpaperID 
    and links_{authorTableName}.citedpaperID = paper_reference_field_labeled.citedpaperID;

select * from links_{authorTableName} INTO OUTFILE 'data/csv/{fieldName}/links_{authorTableName}.csv' 
    FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n';
""")


    print(f"Create and fill links for field author {authorName} with rank {rank}: {authorTableName}")

cursor.close()
conn.close()
