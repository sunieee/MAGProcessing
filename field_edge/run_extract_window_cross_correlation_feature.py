r"""
compute the cross correlation coefficient between the citation count time series of the citing paper and the cited paper
Use the value with the maximal absolute value, among all the sliding windows (default window size is 5 years)
parameters: <citation_link_table_name> <citation_timeseries_table_name> (<window_size> <feature_name>)
==================

"""
from ast import Num
import os
import re
import string
import json
import sys
import numpy
import scipy
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# import MySQLdb
import odbcdb
from odbcdb import *
from scipy.stats import mannwhitneyu
from scipy.stats import ttest_ind

# the overlapping years should be at least MIN_YEAR_SPAN
# should be larger than or equal to window_size
MIN_YEAR_SPAN = 5

# for each citation time series, will truncate the ending curve if it drops below MIN_CITATION_PERCENT * max(series) for MAX_MIN_CITATION_YEAR years
MIN_CITATION_PERCENT = 0.1
MAX_MIN_CITATION_YEAR = 5

# the average citation per year should be at least MIN_AVG_CITATION_PER_YEAR in the compared sub-time-series
MIN_AVG_CITATION_PER_YEAR = 2
MIN_TOTAL_CITATION = 5


def average(vector):
    return sum(vector) / float(len(vector))


def computeTruncatedNum(citation_count_timeseries):

    minCountThre = max(citation_count_timeseries) * MIN_CITATION_PERCENT
    maxCount = max(citation_count_timeseries)

    maxCountIndex = 0
    for i in range(len(citation_count_timeseries)):
        if citation_count_timeseries[i] == maxCount:
            maxCountIndex = i
            break

    belowThreCount = 0
    truncateIndex = len(citation_count_timeseries) - 1
    for i in range(maxCountIndex, len(citation_count_timeseries)):
        if citation_count_timeseries[i] < minCountThre:
            belowThreCount = belowThreCount + 1
        else:
            belowThreCount = 0

        if belowThreCount >= MAX_MIN_CITATION_YEAR:
            truncateIndex = i
            break

    return len(citation_count_timeseries) - 1 - truncateIndex


def computeMaxCrossCorrelation(citingRow, citedRow, window_size):

    citing_totalCitationCount = int(citingRow[2])
    cited_totalCitationCount = int(citedRow[2])
    if (
        citing_totalCitationCount < MIN_TOTAL_CITATION
        or cited_totalCitationCount < MIN_TOTAL_CITATION
    ):
        return None, None

    citing_citeStartYear = int(citingRow[0])
    citing_citeEndYear = int(citingRow[1])
    citing_citationCountByYear = list(map(int, str(citingRow[3]).strip().split(",")))

    cited_citeStartYear = int(citedRow[0])
    cited_citeEndYear = int(citedRow[1])
    cited_citationCountByYear = list(map(int, str(citedRow[3]).strip().split(",")))

    start_year = max(citing_citeStartYear, cited_citeStartYear)
    end_year = min(
        citing_citeEndYear - computeTruncatedNum(citing_citationCountByYear),
        cited_citeEndYear - computeTruncatedNum(cited_citationCountByYear),
    )
    length_timeseries = end_year - start_year + 1

    # the overlapping years should be at least MIN_YEAR_SPAN
    if length_timeseries < MIN_YEAR_SPAN:
        return None, None

    # if illegal data
    if (
        (citing_citeStartYear < cited_citeStartYear)
        or (length_timeseries <= 0)
        or (citing_citeStartYear <= 1900)
        or (cited_citeStartYear <= 1900)
    ):
        print(
            "warning: illegal overlapped citation time series! ID: ",
            str(citing_citeStartYear),
            ",",
            str(cited_citeStartYear),
        )
        return None, None

    citing_timeseries = citing_citationCountByYear[
        start_year
        - citing_citeStartYear : start_year
        - citing_citeStartYear
        + length_timeseries
    ]
    cited_timeseries = cited_citationCountByYear[
        start_year
        - cited_citeStartYear : start_year
        - cited_citeStartYear
        + length_timeseries
    ]

    best_window_offset = None
    max_correlation = 0

    for window_offset in range(0, length_timeseries - window_size + 1):
        timeseries_1 = citing_timeseries[window_offset : window_offset + window_size]
        timeseries_2 = cited_timeseries[window_offset : window_offset + window_size]

        # the average citation per year should be at least MIN_AVG_CITATION_PER_YEAR in the compared sub-time-series
        if (average(timeseries_1) < MIN_AVG_CITATION_PER_YEAR) or (
            average(timeseries_2) < MIN_AVG_CITATION_PER_YEAR
        ):
            continue

        correlation = numpy.corrcoef(timeseries_1, timeseries_2)[0, 1]

        if abs(correlation) >= abs(max_correlation):
            max_correlation = correlation
            best_window_offset = window_offset

    if best_window_offset != None:
        return best_window_offset, max_correlation
    else:
        return None, None


host = "127.0.0.1"
port = "3306"
usr = "root"
pwd = "Vis_2014"

TABLE_NAME = "$$TABLE_NAME$$"
COLUMN_NAME = "$$COLUMN_NAME$$"

selectInheritanceLink = "select citingpaperID, citedpaperID from $$TABLE_NAME$$"


selectGroupOneFeature = "select $$COLUMN_NAME$$ from $$TABLE_NAME$$ where inheritance > 0 and not isnull($$COLUMN_NAME$$);"
selectGroupTwoFeature = "select $$COLUMN_NAME$$ from $$TABLE_NAME$$ where inheritance <= 0 and not isnull($$COLUMN_NAME$$);"

if len(sys.argv) < 4:
    print("Not enough parameters: ", len(sys.argv))
    sys.exit

citation_link_table_name = sys.argv[1]
database = sys.argv[2]
citation_timeseries_table_name = sys.argv[3]

engine = create_engine('mysql+pymysql://root:Vis_2014@localhost:3306/'+database)
sql = '''select citeStartYear, citeEndYear, totalCitationCount, citationCountByYear, paperID from $$TABLE_NAME$$;'''


window_size = 5
feature_name = "window_cross_correlation_feature"

if len(sys.argv) > 4:
    window_size = int(sys.argv[4])

if len(sys.argv) > 5:
    feature_name = sys.argv[5]

selectInheritanceLink = selectInheritanceLink.replace(
    TABLE_NAME, citation_link_table_name
)

sql = sql.replace(TABLE_NAME, citation_timeseries_table_name)
db = pd.read_sql_query(sql, engine)


# conn = MySQLdb.Connection(user=usr, passwd=pwd, db=database, host=host, port=int(port))
conn = ConnectMySQLDB(host, port, database, usr, pwd)
db_cursor = conn.cursor()

db_cursor.execute(selectInheritanceLink)
rows = db_cursor.fetchall()

citingpaperID_list=[]
citedpaperID_list=[]
feature_name_list=[]
for index in range(len(rows)):
    row = rows[index]
    citingpaperID = str(row[0])
    citedpaperID = str(row[1])
    citingpaperID_list.append(citingpaperID)
    citedpaperID_list.append(citedpaperID)

    citingRow = db.loc[db['paperID']==citingpaperID].values.tolist()
    
    if len(citingRow) == 0:
        feature_name_list.append(np.nan)
        # print("citing is none: ", str(citingpaperID))
        continue
    
    citedRow = db.loc[db['paperID']==citedpaperID].values.tolist()

    if len(citedRow) == 0:
        feature_name_list.append(np.nan)
        # print("cited is none: ", str(citedpaperID))
        continue

    window_offset, cross_correlation_feature = computeMaxCrossCorrelation(
        citingRow[0], citedRow[0], window_size
    )

    # if window_offset != None:
    #     cross_correlation_feature = (
    #         str(window_offset) + ":" + str(cross_correlation_feature)
    #     )

    feature_name_list.append(cross_correlation_feature)

    if index % 100 == 0:
        print("Compute features for ", str(index), " features!")

print(len(feature_name_list),len(rows),len(citingpaperID_list),len(citedpaperID_list))

df_feature = pd.DataFrame({"window_cross_correlation_feature": feature_name_list})
df_csv=pd.read_csv("features.csv")
df_feature=pd.concat([df_csv, df_feature], axis=1)
df_feature.to_csv('features.csv',index=False)

db_cursor.close()
conn.close()

# sys.exit

# # reconnect
# # conn = MySQLdb.Connection(user=usr, passwd=pwd, db=database, host=host, port=int(port))
# conn = ConnectMySQLDB(host, port, database, usr, pwd)
# db_cursor = conn.cursor()

# window_offset_one = []
# window_offset_two = []

# features_group_one = []
# db_cursor.execute(selectGroupOneFeature)
# rows = db_cursor.fetchall()

# for row in rows:
#     features_group_one.append(float(str(row[0]).strip().split(":")[1]))
#     window_offset_one.append(int(str(row[0]).strip().split(":")[0]))


# features_group_two = []
# db_cursor.execute(selectGroupTwoFeature)
# rows = db_cursor.fetchall()

# for row in rows:
#     features_group_two.append(float(str(row[0]).strip().split(":")[1]))
#     window_offset_two.append(int(str(row[0]).strip().split(":")[0]))

# print(
#     "feature length:", str(len(features_group_one)), ",", str(len(features_group_two))
# )
# print(
#     "feature average:",
#     str(average(features_group_one)),
#     ",",
#     str(average(features_group_two)),
# )

# print(
#     "absolute feature average:",
#     str(average_abs(features_group_one)),
#     ",",
#     str(average_abs(features_group_two)),
# )

# print(
#     "window offset average:",
#     str(average(window_offset_one)),
#     ",",
#     str(average(window_offset_two)),
# )

# # print("features:", str(features_group_one), ";", str(features_group_two))

# print("feature comparison significance:")

# computeTwoListSignificance(features_group_one, features_group_two)

# print("absolute feature comparison significance:")

# computeTwoListSignificance(
#     [abs(ele) for ele in features_group_one], [abs(ele) for ele in features_group_two]
# )

# db_cursor.close()
# conn.close()
