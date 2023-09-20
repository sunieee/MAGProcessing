# 该代码isKeyPaper仍为0
import os
import re
import string
import json
import sys

# curPath = os.path.abspath(os.path.dirname(__file__))
# rootPath = os.path.split(curPath)[0]
# sys.path.append(curPath)

from odbcdb import *

# settings
# reload(sys)
# sys.setdefaultencoding("utf-8")

host = "127.0.0.1"
port = "3306"
database = "scigene_$$fieldNAME$$_field_pcg"
usr = "root"
pwd = "Vis_2014"

fieldName = "database"

numOfTopAuthors = 1000

field_NAME = "$$fieldNAME$$"

field_AUTHOR = "$$fieldAUTHOR$$"
NUM_TOP_AUTHORS = "$$NUMTOPAUTHORS$$"

#change 1 row, and it is already back
# selectTopfieldAuthors = "select authorID, name, authorRank, PaperCount_field from scigene_$$fieldNAME$$_field.authors_field where authorRank > 200 and authorRank <= $$NUMTOPAUTHORS$$;"
selectTopfieldAuthors = "select authorID, name, authorRank, PaperCount_field from scigene_$$fieldNAME$$_field.authors_field where authorRank > 1000 and authorRank <= $$NUMTOPAUTHORS$$;"
dropfieldPapers = "drop table papers_$$fieldAUTHOR$$"
createfieldPapers = "create table papers_$$fieldAUTHOR$$ (firstAuthorID varchar(15), firstAuthorName varchar(999), isKeyPaper float) select papers_field.paperID, title, year, referenceCount, citationCount, min(authorOrder) as authorOrder, 0 as isKeyPaper, '' as firstAuthorID, '' as firstAuthorName from scigene_$$fieldNAME$$_field.paper_author_field, scigene_$$fieldNAME$$_field.papers_field where authorID = ? and papers_field.paperID = paper_author_field.paperID group by papers_field.paperID, title, year"
createfieldPapersIndex = "create index arc_index on papers_$$fieldAUTHOR$$(paperID)"
updatefieldPaperFirstAuthor = "update papers_$$fieldAUTHOR$$ as P, scigene_$$fieldNAME$$_field.paper_author_field as PA set P.firstAuthorID = PA.authorID where P.paperID = PA.paperID and PA.authorOrder = 1"
updatefieldPaperFirstAuthorName = "update papers_$$fieldAUTHOR$$ as P, scigene_$$fieldNAME$$_field.authors_field as A set P.firstAuthorName = A.name where P.firstAuthorID = A.authorID"
# createfieldPapersIndexMAG = (
#     "create index mag_index on papers_$$fieldAUTHOR$$(MAG_paper_ID)"
# )
dumpfieldPapers = """select * from papers_$$fieldAUTHOR$$ INTO OUTFILE '/home/xiaofengli/PyProject/parser/data/csv/$$fieldNAME$$/papers_$$fieldAUTHOR$$.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n'"""

dropfieldLinks = "drop table links_$$fieldAUTHOR$$"
createfieldLinks = "create table links_$$fieldAUTHOR$$ (extends_prob float) select P.citingpaperID, P.citedpaperID, 0 as sharedAuthor, null as extends_prob from scigene_$$fieldNAME$$_field.paper_reference_field as P where P.citingpaperID in (select paperID from papers_$$fieldAUTHOR$$) group by P.citingpaperID, P.citedpaperID"
createfieldLinkIndex1 = (
    "create index citing_index on links_$$fieldAUTHOR$$(citingpaperID)"
)
createfieldLinkIndex2 = (
    "create index cited_index on links_$$fieldAUTHOR$$(citedpaperID)"
)
updatefieldLinkShareAuthor = "update links_$$fieldAUTHOR$$ as P, scigene_$$fieldNAME$$_field.paper_author_field as A, scigene_$$fieldNAME$$_field.paper_author_field as B set P.sharedAuthor = 1 where A.paperID = P.citingpaperID and B.paperID = P.citedpaperID and A.authorID = B.authorID"
updatefieldLinkInheritance = "update links_$$fieldAUTHOR$$, scigene_$$fieldNAME$$_field.paper_reference_field_labeled set links_$$fieldAUTHOR$$.extends_prob = paper_reference_field_labeled.extends_prob where links_$$fieldAUTHOR$$.citingpaperID = paper_reference_field_labeled.citingpaperID and links_$$fieldAUTHOR$$.citedpaperID = paper_reference_field_labeled.citedpaperID"
dumpfieldLinks = """select * from links_$$fieldAUTHOR$$ INTO OUTFILE '/home/xiaofengli/PyProject/parser/data/csv/$$fieldNAME$$/links_$$fieldAUTHOR$$.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n'"""

if len(sys.argv) >= 2:
    fieldName = str(sys.argv[1])

if len(sys.argv) >= 3:
    numOfTopAuthors = int(sys.argv[2])

# fieldName库authors表前numOfTopAuthors名学者
selectTopfieldAuthors = selectTopfieldAuthors.replace(
    NUM_TOP_AUTHORS, str(numOfTopAuthors)
).replace(field_NAME, fieldName)

print(selectTopfieldAuthors)
database = database.replace(field_NAME, fieldName)
conn = ConnectMySQLDB(host, port, database, usr, pwd)
db_cursor = conn.cursor()

conn_update = ConnectMySQLDB(host, port, database, usr, pwd)
db_update_cursor = conn_update.cursor()

# select all top field authors
db_cursor.execute(selectTopfieldAuthors)
rows = db_cursor.fetchall()

# process each author
for row in rows:
    authorID = str(row[0].strip())
    authorName = str(row[1].strip())
    rank = int(row[2])

    authorTableName = "".join(filter(str.isalpha, authorName)).lower() + str(rank)

    # 删除表papers_学者
    dropfieldPapers_author = dropfieldPapers.replace(field_AUTHOR, authorTableName)
    # 重建表papers_学者
    createfieldPapers_author = createfieldPapers.replace(
        field_AUTHOR, authorTableName
    ).replace(field_NAME, fieldName)
    # 为表papers_学者中paperID加索引
    createfieldPapersIndex_author = createfieldPapersIndex.replace(
        field_AUTHOR, authorTableName
    )

    updatefieldPaperFirstAuthor_author = updatefieldPaperFirstAuthor.replace(
        field_AUTHOR, authorTableName
    ).replace(field_NAME, fieldName)
    updatefieldPaperFirstAuthorName_author = updatefieldPaperFirstAuthorName.replace(
        field_AUTHOR, authorTableName
    ).replace(field_NAME, fieldName)

    # 表papers_学者导入csv
    dumpfieldPapers_author = dumpfieldPapers.replace(
        field_AUTHOR, authorTableName
    ).replace(field_NAME, fieldName)

    # drop arc author db
    try:
        db_update_cursor.execute(dropfieldPapers_author)
    except Exception as e:
        print("No such table:", dropfieldPapers_author)
    conn_update.commit()

    # create arc author db
    db_update_cursor.execute(createfieldPapers_author, authorID)
    conn_update.commit()

    db_update_cursor.execute(createfieldPapersIndex_author)
    conn_update.commit()

    db_update_cursor.execute(updatefieldPaperFirstAuthor_author)
    conn_update.commit()

    db_update_cursor.execute(updatefieldPaperFirstAuthorName_author)
    conn_update.commit()

    # db_update_cursor.execute(dumpfieldPapers_author)

    print(
        "Create and fill papers for field author ",
        authorName,
        " with rank ",
        str(rank),
        ":",
        authorTableName,
    )

    # insert potential influence link table for the author

    dropfieldLinks_author = dropfieldLinks.replace(field_AUTHOR, authorTableName)
    createfieldLinks_author = createfieldLinks.replace(
        field_AUTHOR, authorTableName
    ).replace(field_NAME, fieldName)
    updatefieldLinkShareAuthor_author = updatefieldLinkShareAuthor.replace(
        field_AUTHOR, authorTableName
    ).replace(field_NAME, fieldName)
    createfieldLinkIndex1_author = createfieldLinkIndex1.replace(
        field_AUTHOR, authorTableName
    )
    createfieldLinkIndex2_author = createfieldLinkIndex2.replace(
        field_AUTHOR, authorTableName
    )
    updatefieldLinkInheritance_author = updatefieldLinkInheritance.replace(
        field_AUTHOR, authorTableName
    ).replace(field_NAME, fieldName)
    dumpfieldLinks_author = dumpfieldLinks.replace(
        field_AUTHOR, authorTableName
    ).replace(field_NAME, fieldName)

    # drop arc link db
    try:
        db_update_cursor.execute(dropfieldLinks_author)
    except Exception as e:
        print("No such table:", dropfieldLinks_author)
    conn_update.commit()

    # create arc link db
    db_update_cursor.execute(createfieldLinks_author)
    conn_update.commit()
    db_update_cursor.execute(createfieldLinkIndex1_author)
    conn_update.commit()
    db_update_cursor.execute(createfieldLinkIndex2_author)
    conn_update.commit()
    db_update_cursor.execute(updatefieldLinkShareAuthor_author)
    conn_update.commit()
    db_update_cursor.execute(updatefieldLinkInheritance_author)
    conn_update.commit()

    # db_update_cursor.execute(dumpfieldLinks_author)

    print(
        "Create and fill links for field author ",
        authorName,
        " with rank ",
        str(rank),
        ":",
        authorTableName,
    )

db_cursor.close()
db_update_cursor.close()
conn.close()
conn_update.close()
