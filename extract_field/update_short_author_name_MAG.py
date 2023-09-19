import os
import re
import string
import json
import sys

from gensim import utils
import gensim
from gensim.parsing import preprocessing
import re


def remove_short_tokens(tokens, minsize):
    return [token for token in tokens if len(token) >= minsize]


def strip_short(s, minsize=2):
    s = utils.to_unicode(s)
    return " ".join(remove_short_tokens(s.split(), minsize))


preprocessing.STOPWORDS = set()
preprocessing.DEFAULT_FILTERS[6] = strip_short
preprocessing.DEFAULT_FILTERS.pop()

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

ARC_AUTHOR = "$$ARCAUTHOR$$"
NUM_TOP_AUTHORS = "$$NUMTOPAUTHORS$$"

selectMAGAuthors = "select authorID, name from scigene_acl_anthology.authors_MAG;"
updateMAGAuthor = "update authors_MAG set name_short = ? where authorID = ?"

conn = ConnectMySQLDB(host, port, database, usr, pwd)
db_cursor = conn.cursor()

# select all top ARC authors
db_cursor.execute(selectMAGAuthors)
rows = db_cursor.fetchall()

count = 0

# process each author
for row in rows:

    authorID = str(row[0].strip())
    author = str(row[1].strip())

    # pre-process author name
    author_names = preprocessing.preprocess_string(re.sub("[^\s\w]", "", author))

    # only leave first and last name if any

    if len(author_names) <= 0:
        continue

    authorName = author_names[0]
    if len(author_names) > 1:
        authorName = authorName + " " + author_names[len(author_names) - 1]

    if len(authorName) <= 0:
        continue

    db_cursor.execute(updateMAGAuthor, authorName, authorID)

    count += 1
    if count % 1000 == 0:
        print("Process", count, " authors!")
        conn.commit()

conn.commit()
db_cursor.close()
conn.close()
