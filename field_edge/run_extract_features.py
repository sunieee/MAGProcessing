r"""
compute the cross correlation coefficient between the citation count time series of the citing paper and the cited paper
parameters: $$citation_link_table_name$$ $$citation_timeseries_table_name$$ ($$feature_name$$)
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
from tqdm import tqdm
import math

# import MySQLdb
from utils import *
from scipy.stats import mannwhitneyu
from scipy.stats import ttest_ind

# the overlapping years should be at least MIN_YEAR_SPAN
MIN_YEAR_SPAN = 5

# for each citation time series, will truncate the ending curve if it drops below MIN_CITATION_PERCENT * max(series) for MAX_MIN_CITATION_YEAR years
MIN_CITATION_PERCENT = 0.1
MAX_MIN_CITATION_YEAR = 5

# the average citation per year should be at least MIN_AVG_CITATION_PER_YEAR in the compared sub-time-series
MIN_AVG_CITATION_PER_YEAR = 2
MIN_TOTAL_CITATION = 5

window_size = 5
maxTimeLag = 5


def computeTruncatedNum(citation_count_timeseries):
    """找到从最大引用次数开始，连续低于某个阈值的引用次数的区间，并返回这个区间的长度。
    这可能是为了在某种分析中排除或截断那些在达到最大引用次数后迅速下降的时间序列
    """
    minCountThre = max(citation_count_timeseries) * MIN_CITATION_PERCENT
    # 使用index方法查找maxCount的索引
    maxCountIndex = citation_count_timeseries.index(max(citation_count_timeseries))

    belowThreCount = 0
    for i in range(maxCountIndex, len(citation_count_timeseries)):
        if citation_count_timeseries[i] < minCountThre:
            belowThreCount += 1
        else:
            belowThreCount = 0

        if belowThreCount >= MAX_MIN_CITATION_YEAR:
            return len(citation_count_timeseries) - 1 - i

    return 0


def extract_data(row):
    start_year = int(row[0])
    end_year = int(row[1])
    total_citation_count = int(row[2])
    citation_count_by_year = list(map(int, str(row[3]).strip().split(",")))
    return start_year, end_year, total_citation_count, citation_count_by_year


def computeCrossCorrelation(citing_start, citing_end, cited_start, cited_end, 
                            citing_count_by_year, cited_count_by_year,
                            sliding_window=False):
    start_year = max(citing_start, cited_start)
    end_year = min(citing_end, cited_end)
    length_timeseries = end_year - start_year + 1

    if length_timeseries < MIN_YEAR_SPAN:
        return None

    citing_timeseries = citing_count_by_year[start_year - citing_start : start_year - citing_start + length_timeseries],
    cited_timeseries = cited_count_by_year[start_year - cited_start : start_year - cited_start + length_timeseries]

    if not sliding_window:
        if np.mean(citing_timeseries) < MIN_AVG_CITATION_PER_YEAR or np.mean(cited_timeseries) < MIN_AVG_CITATION_PER_YEAR:
            return None

        return numpy.corrcoef(citing_timeseries, cited_timeseries)[0, 1]

    # best_window_offset = None
    max_correlation = 0
    for window_offset in range(0, length_timeseries - window_size + 1):
        timeseries_1 = citing_timeseries[window_offset : window_offset + window_size]
        timeseries_2 = cited_timeseries[window_offset : window_offset + window_size]

        # the average citation per year should be at least MIN_AVG_CITATION_PER_YEAR in the compared sub-time-series
        if np.mean(timeseries_1) < MIN_AVG_CITATION_PER_YEAR or np.mean(timeseries_2) < MIN_AVG_CITATION_PER_YEAR:
            continue

        correlation = numpy.corrcoef(timeseries_1, timeseries_2)[0, 1]

        if abs(correlation) >= abs(max_correlation):
            max_correlation = correlation
            # best_window_offset = window_offset
    return max_correlation or None


def computeFeatures(citingRow, citedRow):
    citing_start, citing_end, citing_total, citing_count_by_year = extract_data(citingRow)
    cited_start, cited_end, cited_total, cited_count_by_year = extract_data(citedRow)
    
    if (
        citing_total < MIN_TOTAL_CITATION or cited_total < MIN_TOTAL_CITATION
        or citing_start < cited_start
        or citing_start <= 1900 or cited_start <= 1900
    ):
        print(f"warning: illegal overlapped citation time series! ID: {citing_start}, {cited_start}")
        return {}
    
    citingTruncated = computeTruncatedNum(citing_count_by_year)
    citedTruncated = computeTruncatedNum(cited_count_by_year)
    
    if citingTruncated > 0:
        citing_end -= citingTruncated
        citing_count_by_year = citing_count_by_year[:-citingTruncated]

    if citedTruncated > 0:
        cited_end -= citedTruncated
        cited_count_by_year = cited_count_by_year[:-citedTruncated]

    ret = {}
    ret['cross_correlation_feature'] = computeCrossCorrelation(citing_start, citing_end, cited_start, cited_end, 
                                                               citing_count_by_year, cited_count_by_year)
    ret["window_cross_correlation_feature"] = computeCrossCorrelation(citing_start, citing_end, cited_start, cited_end, 
                                                                      citing_count_by_year, cited_count_by_year,
                                                                      sliding_window=True)

    for name, start, end, sliding_window in [('negativetimelagged_cross_correlation_feature', -maxTimeLag, 0, False), 
                             ('timelagged_cross_correlation_feature', 1, maxTimeLag + 1, False),
                             ('window_negativetimelagged_cross_correlation_feature', -maxTimeLag, 0, True), 
                             ('window_timelagged_cross_correlation_feature', 1, maxTimeLag + 1, True)
                             ]:
        # bestTimeLag = None
        maxCorrelation = 0
        for timeLag in range(start, end):
            correlation = computeCrossCorrelation(
                citing_start + timeLag,
                citing_end + timeLag,
                cited_start,
                cited_end,
                citing_count_by_year,
                cited_count_by_year,
                sliding_window=sliding_window
            )

            if correlation and abs(correlation) >= abs(maxCorrelation):
                maxCorrelation = correlation
                # bestTimeLag = timeLag
        ret[name] = maxCorrelation or None
    
    return ret


def get_feature_values(db, paperID):
    data = db.loc[db['paperID'] == paperID]
    if data.empty:
        return None, None
    year = int(data['year'].values[0])
    citation_count = data['citationCount'].values[0]
    if citation_count == -1:
        citation_count = None
    return year, citation_count


def cos_sim(a, b):
    a_norm = np.linalg.norm(a)
    b_norm = np.linalg.norm(b)
    cos = np.dot(a,b)/(a_norm * b_norm)
    return cos


def compute_metrics(citing_list, cited_list):
    union = set(citing_list + cited_list)
    join = set(citing_list) & set(cited_list)

    raw_metric = len(join)
    cosine_metric = (
        0
        if (len(citing_list) <= 0 or len(cited_list) <= 0)
        else raw_metric / math.sqrt(len(citing_list) * len(cited_list))
    )
    jaccard_metric = 0 if len(union) <= 0 else raw_metric / len(union)

    return raw_metric, cosine_metric, jaccard_metric

# 6 correlation features
timeseries_df = pd.read_sql_query(f"""select citeStartYear, citeEndYear, totalCitationCount, citationCountByYear, paperID from {citation_timeseries_table};""", engine)
reference_rows = executeFetch(f"select citingpaperID, citedpaperID from {citation_link_table}")

# 5 other features
papers_df = pd.read_sql_query(f"""select * from papers_field;""", engine)
paper_author_df = pd.read_sql_query(f"""select * from paper_author_field;""", engine)
df_csv = pd.read_csv('similarity_features.csv')
df_csv.pop('title')
df_csv.pop('abstract')

# 6 citation network features
reference_df = pd.read_sql_query(f"select citingpaperID, citedpaperID from {citation_link_table}", engine)
# 为citingpaperID和citedpaperID分别建立索引
df_by_citing = reference_df.set_index('citingpaperID')
df_by_cited = reference_df.set_index('citedpaperID')

df_feature = pd.DataFrame(columns=["cross_correlation_feature", 'negativetimelagged_cross_correlation_feature', 'timelagged_cross_correlation_feature',
                                   "window_cross_correlation_feature", 'window_negativetimelagged_cross_correlation_feature', 'window_timelagged_cross_correlation_feature',
                                   'year_difference', 'citingpaperCitationCount', 'citedpaperCitationCount', 'self_cite',
                                   'similarity',
                                   "raw_cocitation_feature", "cosine_cocitation_feature", "jaccard_cocitation_feature", 
                                   "raw_bibcoupling_feature", "cosine_bibcoupling_feature", "jaccard_bibcoupling_feature"])
df_feature.index.names = ["citingpaperID", "citedpaperID"]


for row in tqdm(reference_rows):
    citing = str(row[0])
    cited = str(row[1])
    
    citingRow = timeseries_df.loc[timeseries_df['paperID'] == citing].values.tolist()
    citedRow = timeseries_df.loc[timeseries_df['paperID'] == cited].values.tolist()

    if len(citingRow)==0 or len(citedRow)==0:
        feature = None
    else:
        feature = computeCrossCorrelation(citingRow[0], citedRow[0])
    
    
    year_citing, num_citing = get_feature_values(papers_df, citing)
    year_cited, num_cited = get_feature_values(papers_df, cited)
    
    year_diff = year_citing - year_cited if year_citing and year_cited and year_citing >= year_cited else None
    
    citing_authors = set(paper_author_df.loc[paper_author_df['paperID'] == citing]['authorID'].values.tolist())
    cited_authors = set(paper_author_df.loc[paper_author_df['paperID'] == cited]['authorID'].values.tolist())
    self_cite_count = len(citing_authors.intersection(cited_authors))

    db_citing=df_csv.loc[df_csv['paperID']==citing]
    db_cited=df_csv.loc[df_csv['paperID']==cited]
    if len(db_citing.values)==0 or len(db_cited.values)==0:
        similarity = None
    else:
        similarity=cos_sim(db_citing.iloc[0][1:], db_cited.iloc[0][1:])
    
    other_feature = {
        'year_difference': year_diff,
        'citingpaperCitationCount': num_citing,
        'citedpaperCitationCount': num_cited,
        'self_cite': self_cite_count if citing_authors and cited_authors else None,
        'similarity': similarity
    }

    # 使用索引查询co-citation数据
    citingCitations = df_by_cited.loc[cited]['citingpaperID'].tolist() if cited in df_by_cited.index else []
    citedCitations = df_by_cited.loc[citing]['citingpaperID'].tolist() if citing in df_by_cited.index else []

    raw_cocitation, cosine_cocitation, jaccard_cocitation = compute_metrics(citingCitations, citedCitations)

    # 使用索引查询bibliometric coupling数据
    citingReferences = df_by_citing.loc[citing]['citedpaperID'].tolist() if citing in df_by_citing.index else []
    citedReferences = df_by_citing.loc[cited]['citedpaperID'].tolist() if cited in df_by_citing.index else []

    raw_bibcoupling, cosine_bibcoupling, jaccard_bibcoupling = compute_metrics(citingReferences, citedReferences)

    network_features = {
        'raw_cocitation_feature': raw_cocitation,
        'cosine_cocitation_feature': cosine_cocitation,
        'jaccard_cocitation_feature': jaccard_cocitation,
        'raw_bibcoupling_feature': raw_bibcoupling,
        'cosine_bibcoupling_feature': cosine_bibcoupling,
        'jaccard_bibcoupling_feature': jaccard_bibcoupling
    }

    df_feature.loc[(citing, cited)] = {
        **feature,
        **other_feature,
        **network_features
    }


print(df_feature)
# sleep()
df_feature.to_csv('all_features.csv',index=True)

cursor.close()
conn.close()
