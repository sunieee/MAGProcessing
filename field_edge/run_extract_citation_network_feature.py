r"""
compute the 6 citation network features between the citing paper and the cited paper: co-citation, bib-coupling (raw, cosine, jaccard)
parameters: <citation_link_table_name_for_features> <all_citation_link_table_name>
actual parameters: feature_network scigene_acl_anthology.paper_reference_ARC
==================

"""
import pandas as pd
from ast import Num
import os
import re
import string
import json
import sys
import math

# import MySQLdb
import odbcdb
from odbcdb import *
from scipy.stats import mannwhitneyu
from scipy.stats import ttest_ind


host = "127.0.0.1"
port = "3306"
usr = "root"
pwd = "Vis_2014"

TABLE_NAME = "$$TABLE_NAME$$"
COLUMN_NAME = "$$COLUMN_NAME$$"
ALL_CITATION_TABLE_NAME = "$$ALL_CITATION_TABLE_NAME$$"

selectInheritanceLink = "select citingpaperID, citedpaperID from $$TABLE_NAME$$"
selectCitations = (
    "select citingpaperID from $$ALL_CITATION_TABLE_NAME$$ where citedpaperID = ?"
)
selectReferences = (
    "select citedpaperID from $$ALL_CITATION_TABLE_NAME$$ where citingpaperID = ?"
)
selectGroupOneFeature = "select $$COLUMN_NAME$$ from $$TABLE_NAME$$ where inheritance > 0 and not isnull($$COLUMN_NAME$$);"
selectGroupTwoFeature = "select $$COLUMN_NAME$$ from $$TABLE_NAME$$ where inheritance <= 0 and not isnull($$COLUMN_NAME$$);"

if len(sys.argv) < 4:
    print("Not enough parameters: ", len(sys.argv))
    sys.exit

citation_link_table_name = sys.argv[1]
database=sys.argv[2]
all_citation_table_name = sys.argv[3]

feature_names = [
    "raw_cocitation_feature",
    "cosine_cocitation_feature",
    "jaccard_cocitation_feature",
    "raw_bibcoupling_feature",
    "cosine_bibcoupling_feature",
    "jaccard_bibcoupling_feature",
]

selectInheritanceLink = selectInheritanceLink.replace(
    TABLE_NAME, citation_link_table_name
)
selectCitations = selectCitations.replace(
    ALL_CITATION_TABLE_NAME, all_citation_table_name
)
selectReferences = selectReferences.replace(
    ALL_CITATION_TABLE_NAME, all_citation_table_name
)

# conn = MySQLdb.Connection(user=usr, passwd=pwd, db=database, host=host, port=int(port))
conn = ConnectMySQLDB(host, port, database, usr, pwd)
db_cursor = conn.cursor()

db_cursor.execute(selectInheritanceLink)
rows = db_cursor.fetchall()

feature_name_list=[]
for index in range(len(rows)):
    row = rows[index]
    citingpaperID = str(row[0])
    citedpaperID = str(row[1])

    # co-citation metrics
    db_cursor.execute(selectCitations, citingpaperID)
    citingPaperRows = db_cursor.fetchall()
    citingCitations = (
        [] if len(citingPaperRows) <= 0 else [str(x[0]) for x in citingPaperRows]
    )
    # print(citingCitations)
    db_cursor.execute(selectCitations, citedpaperID)
    citedPaperRows = db_cursor.fetchall()
    citedCitations = (
        [] if len(citedPaperRows) <= 0 else [str(x[0]) for x in citedPaperRows]
    )
    # print(citedCitations)
    # s()
    union = set(citingCitations + citedCitations)
    join = set(citingCitations) & set(citedCitations)

    raw_cocitation = len(join)
    cosine_cocitation = (
        0
        if (len(citingCitations) <= 0 or len(citedCitations) <= 0)
        else raw_cocitation / math.sqrt(len(citingCitations) * len(citedCitations))
    )
    jaccard_cocitation = 0 if len(union) <= 0 else raw_cocitation / len(union)

    # bibliometric coupling metrics
    db_cursor.execute(selectReferences, citingpaperID)
    citingReferenceRows = db_cursor.fetchall()
    citingReferences = (
        []
        if len(citingReferenceRows) <= 0
        else [str(x[0]) for x in citingReferenceRows]
    )

    db_cursor.execute(selectReferences, citedpaperID)
    citedReferenceRows = db_cursor.fetchall()
    citedReferences = (
        [] if len(citedReferenceRows) <= 0 else [str(x[0]) for x in citedReferenceRows]
    )

    union = set(citingReferences + citedReferences)
    join = set(citingReferences) & set(citedReferences)

    raw_bibcoupling = len(join)
    cosine_bibcoupling = (
        0
        if (len(citingReferences) <= 0 or len(citedReferences) <= 0)
        else raw_bibcoupling / math.sqrt(len(citingReferences) * len(citedReferences))
    )
    jaccard_bibcoupling = 0 if len(union) <= 0 else raw_bibcoupling / len(union)

    feature_name_list.append([raw_cocitation,cosine_cocitation,jaccard_cocitation,raw_bibcoupling,cosine_bibcoupling,jaccard_bibcoupling])
    # db_cursor_update.execute(
    #     updateFeature,
    #     raw_cocitation,
    #     cosine_cocitation,
    #     jaccard_cocitation,
    #     raw_bibcoupling,
    #     cosine_bibcoupling,
    #     jaccard_bibcoupling,
    #     citingpaperID,
    #     citedpaperID,
    # )
    # conn_update.commit()

    if index % 1000 == 0:
        print("Compute features for ", str(index), " features!")

df_feature = pd.DataFrame(feature_name_list,columns=['raw_cocitation','cosine_cocitation','jaccard_cocitation','raw_bibcoupling','cosine_bibcoupling','jaccard_bibcoupling'])
print(df_feature)
df_csv=pd.read_csv("all_features.csv")
df_feature=pd.concat([df_csv, df_feature], axis=1)
df_feature.to_csv('all_features.csv',index=False)

db_cursor.close()
conn.close()
