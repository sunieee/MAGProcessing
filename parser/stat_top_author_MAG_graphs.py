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

selectTopfieldAuthors = "select authorID, name, authorRank, PaperCount_field from scigene_$$fieldNAME$$_field.authors_field where authorRank <= $$NUMTOPAUTHORS$$;"
selectfieldPapers = (
    "select year, citationCount, authorOrder, isKeyPaper from papers_$$fieldAUTHOR$$"
)
selectfieldLinks = "select sharedAuthor, extends_prob from links_$$fieldAUTHOR$$"

if len(sys.argv) >= 2:
    fieldName = str(sys.argv[1])

if len(sys.argv) >= 3:
    numOfTopAuthors = int(sys.argv[2])

database = database.replace(field_NAME, fieldName)
selectTopfieldAuthors = selectTopfieldAuthors.replace(
    NUM_TOP_AUTHORS, str(numOfTopAuthors)
).replace(field_NAME, fieldName)

conn = ConnectMySQLDB(host, port, database, usr, pwd)
db_cursor = conn.cursor()

# select all top field authors
db_cursor.execute(selectTopfieldAuthors)
rows = db_cursor.fetchall()

all_year_span = 0
all_year_count = 0
all_paper_count = 0
all_valid_citation_ratio = 0
all_valid_citation_num = 0
all_first_author_ratio = 0
all_key_paper_50_ratio = 0
all_key_paper_60_ratio = 0
all_key_paper_70_ratio = 0
all_key_paper_80_ratio = 0
all_key_paper_90_ratio = 0
all_key_paper_100_ratio = 0

all_link_count = 0
all_link_paper_ratio = 0
all_shareauthor_ratio = 0
all_valid_inheritance_ratio = 0
all_inheritance_50_ratio = 0
all_inheritance_100_ratio = 0

all_none_inheritance = 0
all_negative_inheritance = 0

# process each author
for row in rows:
    authorID = str(row[0].strip())
    authorName = str(row[1].strip())
    rank = int(row[2])

    authorTableName = "".join(filter(str.isalpha, authorName)).lower() + str(rank)

    selectfieldPapers_author = selectfieldPapers.replace(field_AUTHOR, authorTableName)
    selectfieldLinks_author = selectfieldLinks.replace(field_AUTHOR, authorTableName)

    # select all papers of the author
    db_cursor.execute(selectfieldPapers_author)
    paper_rows = db_cursor.fetchall()

    min_year = 3000
    max_year = 0
    year_map = {}
    paper_count = len(paper_rows)
    total_valid_citation_count = 0
    sum_valid_citations = 0
    total_first_author_count = 0
    total_key_paper_50_count = 0
    total_key_paper_60_count = 0
    total_key_paper_70_count = 0
    total_key_paper_80_count = 0
    total_key_paper_90_count = 0
    total_key_paper_100_count = 0

    for paper_row in paper_rows:

        year = int(paper_row[0])
        citationCount = int(paper_row[1])
        authorOrder = int(paper_row[2])
        isKeyPaper = float(paper_row[3])

        if year < min_year:
            min_year = year

        if year > max_year:
            max_year = year

        year_map[year] = 1

        if citationCount >= 0:
            total_valid_citation_count += 1
            sum_valid_citations += citationCount

        if authorOrder == 1:
            total_first_author_count += 1

        if isKeyPaper >= 0.5:
            total_key_paper_50_count += 1

        if isKeyPaper >= 0.6:
            total_key_paper_60_count += 1

        if isKeyPaper >= 0.7:
            total_key_paper_70_count += 1

        if isKeyPaper >= 0.8:
            total_key_paper_80_count += 1

        if isKeyPaper >= 0.9:
            total_key_paper_90_count += 1

        if isKeyPaper >= 1.0:
            total_key_paper_100_count += 1

    all_year_span += max_year - min_year + 1
    all_year_count += len(year_map.keys())
    all_paper_count += paper_count
    all_valid_citation_ratio += total_valid_citation_count / paper_count
    all_valid_citation_num += sum_valid_citations / total_valid_citation_count
    all_first_author_ratio += total_first_author_count / paper_count
    all_key_paper_50_ratio += total_key_paper_50_count / paper_count
    all_key_paper_60_ratio += total_key_paper_60_count / paper_count
    all_key_paper_70_ratio += total_key_paper_70_count / paper_count
    all_key_paper_80_ratio += total_key_paper_80_count / paper_count
    all_key_paper_90_ratio += total_key_paper_90_count / paper_count
    all_key_paper_100_ratio += total_key_paper_100_count / paper_count

    # select all links of the author
    db_cursor.execute(selectfieldLinks_author)
    link_rows = db_cursor.fetchall()

    link_count = len(link_rows)
    total_shareauthor_count = 0
    total_valid_inheritance_count = 0
    total_inheritance_50_count = 0
    total_inheritance_100_count = 0

    for link_row in link_rows:
        sharedAuthor = int(str(link_row[0]).strip())

        inheritance = 0
        if link_row[1] == None:
            all_none_inheritance += 1
        else:
            inheritance = float(str(link_row[1]).strip())

        if sharedAuthor > 0:
            total_shareauthor_count += 1

        if inheritance >= 0:
            total_valid_inheritance_count += 1
        else:
            all_negative_inheritance += 1

        if inheritance >= 0.5:
            total_inheritance_50_count += 1

        if inheritance >= 1:
            total_inheritance_100_count += 1

    all_link_count += link_count
    if(link_count==0):
        continue
    all_shareauthor_ratio += total_shareauthor_count / link_count
    all_valid_inheritance_ratio += total_valid_inheritance_count / link_count
    all_inheritance_50_ratio += (
        total_inheritance_50_count / total_valid_inheritance_count
        if total_valid_inheritance_count > 0
        else 0
    )
    all_inheritance_100_ratio += (
        total_inheritance_100_count / total_valid_inheritance_count
        if total_valid_inheritance_count > 0
        else 0
    )

    all_link_paper_ratio += link_count / paper_count

db_cursor.close()
conn.close()

print("====== Total paper statistics of top ", numOfTopAuthors, " authors ======")

print("avg_year_span: ", all_year_span / numOfTopAuthors)
print("avg_year_count: ", all_year_count / numOfTopAuthors)
print("avg_paper_count: ", all_paper_count / numOfTopAuthors)
print("avg_valid_citation_ratio: ", all_valid_citation_ratio / numOfTopAuthors)
print("avg_valid_citation_num: ", all_valid_citation_num / numOfTopAuthors)
print("avg_first_author_ratio: ", all_first_author_ratio / numOfTopAuthors)
print("avg_key_paper_50_ratio: ", all_key_paper_50_ratio / numOfTopAuthors)
print("avg_key_paper_60_ratio: ", all_key_paper_60_ratio / numOfTopAuthors)
print("avg_key_paper_70_ratio: ", all_key_paper_70_ratio / numOfTopAuthors)
print("avg_key_paper_80_ratio: ", all_key_paper_80_ratio / numOfTopAuthors)
print("avg_key_paper_90_ratio: ", all_key_paper_90_ratio / numOfTopAuthors)
print("avg_key_paper_100_ratio: ", all_key_paper_100_ratio / numOfTopAuthors)

print("====== Total link statistics of top ", numOfTopAuthors, " authors ======")

print("avg_link_count: ", all_link_count / numOfTopAuthors)
print("avg_link_paper_ratio: ", all_link_paper_ratio / numOfTopAuthors)
print("avg_shareauthor_ratio: ", all_shareauthor_ratio / numOfTopAuthors)
print("avg_valid_inheritance_ratio: ", all_valid_inheritance_ratio / numOfTopAuthors)
print("avg_inheritance_50_ratio: ", all_inheritance_50_ratio / numOfTopAuthors)
print("avg_inheritance_100_ratio: ", all_inheritance_100_ratio / numOfTopAuthors)
print("all_none_inheritance: ", all_none_inheritance)
