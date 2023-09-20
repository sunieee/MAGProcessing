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

#change 2 rows
selectTopfieldAuthors = "select authorID, name, authorRank, PaperCount_field from scigene_$$fieldNAME$$_field.authors_field where authorRank > $$NUMTOPAUTHORS$$ and authorRank <= 250 and FellowType is null;"
dumpTopfieldAuthors = """select * from scigene_$$fieldNAME$$_field.authors_field where authorRank > $$NUMTOPAUTHORS$$ and authorRank <= 250 and FellowType is null INTO OUTFILE '/home/leishi/scigene/dataset/MAG/parser/data/csv/$$fieldNAME$$/top_$$NUMTOPAUTHORS$$_field_authors_add.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n'"""
dumpfieldPapers = """select * from papers_$$fieldAUTHOR$$ INTO OUTFILE '/home/leishi/scigene/dataset/MAG/parser/data/csv/$$fieldNAME$$/papers_$$fieldAUTHOR$$.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n'"""
dumpfieldLinks = """select * from links_$$fieldAUTHOR$$ INTO OUTFILE '/home/leishi/scigene/dataset/MAG/parser/data/csv/$$fieldNAME$$/links_$$fieldAUTHOR$$.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n'"""

if len(sys.argv) >= 2:
    fieldName = str(sys.argv[1])

if len(sys.argv) >= 3:
    numOfTopAuthors = int(sys.argv[2])

database = database.replace(field_NAME, fieldName)
selectTopfieldAuthors = selectTopfieldAuthors.replace(
    NUM_TOP_AUTHORS, str(numOfTopAuthors)
).replace(field_NAME, fieldName)
dumpTopfieldAuthors = dumpTopfieldAuthors.replace(
    NUM_TOP_AUTHORS, str(numOfTopAuthors)
).replace(field_NAME, fieldName)

conn = ConnectMySQLDB(host, port, database, usr, pwd)
db_cursor = conn.cursor()

conn_update = ConnectMySQLDB(host, port, database, usr, pwd)
db_update_cursor = conn_update.cursor()

# dump all top field authors
db_update_cursor.execute(dumpTopfieldAuthors)

# select all top field authors
db_cursor.execute(selectTopfieldAuthors)
rows = db_cursor.fetchall()

# process each author
for row in rows:
    authorID = str(row[0].strip())
    authorName = str(row[1].strip())
    rank = int(row[2])

    authorTableName = "".join(filter(str.isalpha, authorName)).lower() + str(rank)

    dumpfieldPapers_author = dumpfieldPapers.replace(
        field_AUTHOR, authorTableName
    ).replace(field_NAME, fieldName)
    dumpfieldLinks_author = dumpfieldLinks.replace(
        field_AUTHOR, authorTableName
    ).replace(field_NAME, fieldName)

    db_update_cursor.execute(dumpfieldPapers_author)
    db_update_cursor.execute(dumpfieldLinks_author)

    print(
        "Dump papers and links for field author ",
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

import pandas as pd

old = pd.read_csv('/home/leishi/scigene/dataset/MAG/parser/data/csv/'+fieldName+'/top_200_field_authors.csv',header=None)
new = pd.read_csv('/home/leishi/scigene/dataset/MAG/parser/data/csv/'+fieldName+'/top_200_field_authors_add.csv',header=None)
old = pd.concat([old,new],axis=0)
print(old)
old.to_csv('/home/leishi/scigene/dataset/MAG/parser/data/csv/'+fieldName+'/top_200_field_authors.csv',index=None,header=0)