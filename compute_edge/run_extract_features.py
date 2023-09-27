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

    citing_timeseries = citing_count_by_year[start_year - citing_start : start_year - citing_start + length_timeseries]
    cited_timeseries = cited_count_by_year[start_year - cited_start : start_year - cited_start + length_timeseries]

    assert len(citing_timeseries) == len(cited_timeseries) == length_timeseries

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
    
    if citing_total < MIN_TOTAL_CITATION or cited_total < MIN_TOTAL_CITATION:
        # print(f"warning: too small citation count! {citing_total}, {cited_total}")
        return {}
    if citing_start < cited_start or citing_start <= 1900 or cited_start <= 1900:
        print(f"warning: illegal overlapped citation time series! {citing_start}, {cited_start}")
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
    ret['cross_correlation'] = computeCrossCorrelation(citing_start, citing_end, cited_start, cited_end, 
                                                               citing_count_by_year, cited_count_by_year)
    ret["window_cross_correlation"] = computeCrossCorrelation(citing_start, citing_end, cited_start, cited_end, 
                                                                      citing_count_by_year, cited_count_by_year,
                                                                      sliding_window=True)

    for name, start, end, sliding_window in [('negativetimelagged_cross_correlation', -maxTimeLag, 0, False), 
                             ('timelagged_cross_correlation', 1, maxTimeLag + 1, False),
                             ('window_negativetimelagged_cross_correlation', -maxTimeLag, 0, True), 
                             ('window_timelagged_cross_correlation', 1, maxTimeLag + 1, True)
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


def get_values_from_index(df, index, column_name):
    if index in df.index:
        result = df.loc[index]
        if isinstance(result, pd.DataFrame):
            return result[column_name].tolist()
        elif isinstance(result, pd.Series):
            return [result[column_name]]
    return []
    

# 6 correlation features
timeseries_df = pd.read_sql_query(f"""select citeStartYear, citeEndYear, totalCitationCount, citationCountByYear, paperID 
                                  from {citation_timeseries_table}
                                  where paperID in {nodes}""", engine)

# 5 other features
papers_df = pd.read_sql_query(f"""select * from papers_field where paperID in {nodes};""", engine)
paper_author_df = pd.read_sql_query(f"""select * from paper_author_field where paperID in {nodes};""", engine)
similarity_df = pd.read_csv('out/similarity_features.csv')
similarity_df.pop('title')
similarity_df.pop('abstract')
similarity_df['paperID'] = similarity_df['paperID'].astype(str)
similarity_df.set_index('paperID', inplace=True)

# 6 citation network features
feature_names = ["cross_correlation", 'negativetimelagged_cross_correlation', 'timelagged_cross_correlation',
                "window_cross_correlation", 'window_negativetimelagged_cross_correlation', 'window_timelagged_cross_correlation',
                'year_diff', 'citing_paper_citationcount', 'cited_paper_citationcount', 'self_cite',
                'similarity',
                "raw_cocitation", "cosine_cocitation", "jaccard_cocitation", 
                "raw_bibcoupling", "cosine_bibcoupling", "jaccard_bibcoupling"]
df_feature = pd.DataFrame(columns=feature_names)
# df_feature.index.names = ["citingpaperID", "citedpaperID"]


for i in tqdm(range(len(edges))):
    row = edges.iloc[i]
    authorID = row['authorID']
    citing = row['citingpaperID']
    cited = row['citedpaperID']
    
    citingRow = timeseries_df.loc[timeseries_df['paperID'] == citing].values.tolist()
    citedRow = timeseries_df.loc[timeseries_df['paperID'] == cited].values.tolist()

    if len(citingRow)==0 or len(citedRow)==0:
        feature = {}
    else:
        feature = computeFeatures(citingRow[0], citedRow[0])
    
    
    year_citing, num_citing = get_feature_values(papers_df, citing)
    year_cited, num_cited = get_feature_values(papers_df, cited)
    
    year_diff = year_citing - year_cited if year_citing and year_cited and year_citing >= year_cited else None
    
    citing_authors = set(paper_author_df.loc[paper_author_df['paperID'] == citing]['authorID'].values.tolist())
    cited_authors = set(paper_author_df.loc[paper_author_df['paperID'] == cited]['authorID'].values.tolist())
    self_cite_count = len(citing_authors.intersection(cited_authors))
    
    other_feature = {
        'year_diff': year_diff,
        'citing_paper_citationcount': num_citing,
        'cited_paper_citationcount': num_cited,
        'self_cite': self_cite_count if citing_authors and cited_authors else None,
        'similarity': cos_sim(similarity_df.loc[citing], similarity_df.loc[cited])
    }

    # 使用索引查询co-citation数据
    citingCitations = get_values_from_index(edges_by_cited, cited, 'citingpaperID')
    citedCitations = get_values_from_index(edges_by_cited, citing, 'citingpaperID')

    raw_cocitation, cosine_cocitation, jaccard_cocitation = compute_metrics(citingCitations, citedCitations)

    # 使用索引查询bibliometric coupling数据
    citingReferences = get_values_from_index(edges_by_citing, citing, 'citedpaperID')
    citedReferences = get_values_from_index(edges_by_citing, cited, 'citedpaperID')

    raw_bibcoupling, cosine_bibcoupling, jaccard_bibcoupling = compute_metrics(citingReferences, citedReferences)

    network_features = {
        'raw_cocitation': raw_cocitation,
        'cosine_cocitation': cosine_cocitation,
        'jaccard_cocitation': jaccard_cocitation,
        'raw_bibcoupling': raw_bibcoupling,
        'cosine_bibcoupling': cosine_bibcoupling,
        'jaccard_bibcoupling': jaccard_bibcoupling
    }

    dic = {
        **feature,
        **other_feature,
        **network_features
    }
    for feature in feature_names:
        if feature not in dic or dic[feature] is None:
            dic[feature] = np.nan

    df_feature.loc[citing + ' ' + cited + ' ' + authorID] = dic

# 假设df_feature已经有一个MultiIndex
# 拆分MultiIndex为两个单独的列
df_feature['citingpaperID'], df_feature['citedpaperID'], df_feature['authorID'] = zip(*df_feature.index.str.split(' '))

# 将这两列放在DataFrame的前面
cols = ['citingpaperID', 'citedpaperID', 'authorID'] + [col for col in df_feature if col not in ['citingpaperID', 'citedpaperID', 'authorID']]
df_feature = df_feature[cols]

# sleep()
df_feature.to_csv('out/all_features.csv',index=False)
print('all_features.csv saved', len(df_feature))
print(df_feature.head())

cursor.close()
conn.close()
