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
database = "scigene_acl_anthology"
usr = "root"
pwd = "Vis_2014"

bypass_pattern = ["{", "'", "}", "\\", "/", "-", ".", '"']

MAG_ARC_MIN_PAPER_RATIO = 0.5
FIRST_SECOND_MIN_PAPER_RATIO = 5
numOfTopAuthors = 1000

ARC_AUTHOR = "$$ARCAUTHOR$$"
NUM_TOP_AUTHORS = "$$NUMTOPAUTHORS$$"

selectTopARCAuthors = "select authorID, name, PaperCount_ARC, authorRank from scigene_acl_anthology.authors_ARC where authorRank <= $$NUMTOPAUTHORS$$;"
selectDupMAGAuthors = "select authorID, name_short, PaperCount_NLP from authors_MAG where name_short like ? order by papercount_NLP desc limit 2"
selectARCAuthorCitations = "select P.CitationCount from papers_ARC as P join paper_author_ARC as PA on PA.authorID = ? and P.paperID = PA.paperID;"
updateTopARCAuthor = "update authors_ARC set name_MAG = ?, authorID_MAG = ?, PaperCount_MAG_NLP = ?, CitationCount_MAG_NLP = ?, hIndex_MAG_NLP = ? where authorID = ?"
updateTopARCAuthorHIndex = "update authors_ARC set hIndex_ARC = ? where authorID = ?"

# selectARCAuthorCitationCount = "select sum(P.citationCount) from papers_ARC as P join paper_author_ARC as PA on PA.authorID = ? and P.paperID = PA.paperID and P.citationCount >=0;"
selectARCAuthorMAGCitationCount = "select sum(P.CitationCount) from papers_MAG as P join paper_author_MAG as PA on PA.authorID = ? and P.paperID = PA.paperID;"
selectARCAuthorMAGCitations = "select P.CitationCount from papers_MAG as P join paper_author_MAG as PA on PA.authorID = ? and P.paperID = PA.paperID;"

if len(sys.argv) >= 2:
    numOfTopAuthors = int(sys.argv[1])

selectTopARCAuthors = selectTopARCAuthors.replace(NUM_TOP_AUTHORS, str(numOfTopAuthors))

conn = ConnectMySQLDB(host, port, database, usr, pwd)
db_cursor = conn.cursor()

# select all top ARC authors
db_cursor.execute(selectTopARCAuthors)
rows = db_cursor.fetchall()

# ARC_MAG author id map
ARC_MAG_author_id_map = {}

# process each author
for row in rows:

    authorID = str(row[0].strip())
    authorName = str(row[1].strip())
    authorPaperCount = int(row[2])
    authorRank = int(row[3])

    # compute hIndex_ARC
    hIndex_ARC = 0

    citations = []
    db_cursor.execute(selectARCAuthorCitations, authorID)
    citation_rows = db_cursor.fetchall()
    for citation_row in citation_rows:
        citations.append(int(citation_row[0]))
    citations.sort(reverse=True)

    for citation_count in citations:
        if citation_count > hIndex_ARC:
            hIndex_ARC += 1
        else:
            break

    db_cursor.execute(
        updateTopARCAuthorHIndex,
        hIndex_ARC,
        authorID,
    )
    conn.commit()

    # find potential duplicate names in MAG
    db_cursor.execute(selectDupMAGAuthors, authorName)
    name_rows = db_cursor.fetchall()

    if len(name_rows) <= 0:
        continue

    authorID_MAG = str(name_rows[0][0].strip())
    authorName_MAG = str(name_rows[0][1].strip())
    authorPaperCount_MAG_NLP = int(name_rows[0][2])

    # only find one author in MAG
    if len(name_rows) == 1:

        # paper count of this author is too small
        if (authorPaperCount_MAG_NLP / authorPaperCount) < MAG_ARC_MIN_PAPER_RATIO:
            print(
                "Bypass potential duplicate author because of small NLP papers: ",
                authorName,
                authorPaperCount,
                authorName_MAG,
                authorPaperCount_MAG_NLP,
            )
            continue
    else:
        # find at least two authors in MAG
        top_dup_authorID_MAG = str(name_rows[1][0].strip())
        top_dup_authorName_MAG = str(name_rows[1][1].strip())
        top_dup_authorPaperCount_MAG_NLP = int(name_rows[1][2])

        if (
            (authorPaperCount_MAG_NLP / authorPaperCount) < MAG_ARC_MIN_PAPER_RATIO
        ) or (
            (authorPaperCount_MAG_NLP / top_dup_authorPaperCount_MAG_NLP)
            < FIRST_SECOND_MIN_PAPER_RATIO
        ):
            print(
                "Bypass potential duplicate author because of small NLP papers or duplicate names: ",
                authorName,
                authorPaperCount,
                authorName_MAG,
                authorPaperCount_MAG_NLP,
                top_dup_authorName_MAG,
                top_dup_authorPaperCount_MAG_NLP,
            )
            continue

    # compute CitationCount_MAG_NLP
    CitationCount_MAG_NLP = -1

    db_cursor.execute(selectARCAuthorMAGCitationCount, authorID_MAG)
    citation_rows = db_cursor.fetchall()

    if len(citation_rows) > 0:
        CitationCount_MAG_NLP = int(citation_rows[0][0])

    # compute hIndex_MAG_NLP
    hIndex_MAG_NLP = 0

    citations = []
    db_cursor.execute(selectARCAuthorMAGCitations, authorID_MAG)
    citation_rows = db_cursor.fetchall()
    for citation_row in citation_rows:
        citations.append(int(citation_row[0]))
    citations.sort(reverse=True)

    for citation_count in citations:
        if citation_count > hIndex_MAG_NLP:
            hIndex_MAG_NLP += 1
        else:
            break

    db_cursor.execute(
        updateTopARCAuthor,
        authorName_MAG,
        authorID_MAG,
        authorPaperCount_MAG_NLP,
        CitationCount_MAG_NLP,
        hIndex_MAG_NLP,
        authorID,
    )
    conn.commit()

    print("Process author: ", authorName, " with rank ", str(authorRank))

db_cursor.close()
conn.close()
