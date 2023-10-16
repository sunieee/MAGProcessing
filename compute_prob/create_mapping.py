from utils import *
import json
import datetime
import pandas as pd

MIN_STUDENT_AUTHOR_ORDER = 3

df_paper_author_field_filtered = df_paper_author_field[df_paper_author_field['authorID'].isin(authorID_list)]
df_paper_author_field_filtered = df_paper_author_field_filtered[['authorID', 'paperID', 'authorOrder']].drop_duplicates()

# df_paper_author_field_filtered = top_field_authors_df.merge(df_paper_author_field, on="authorID")
# df_paper_author_field_filtered = df_paper_author_field_filtered[['authorID', 'paperID', 'authorOrder']].drop_duplicates()

# Creating the firstAuthorTmp DataFrame
print("Pre-compute first author maps!", datetime.datetime.now().strftime("%H:%M:%S"))
firstAuthorTmp = df_paper_author_field_filtered.merge(df_paper_author_field, on="paperID", suffixes=('', '_first')) \
    .query("authorOrder > 1 and authorOrder_first == 1") 
    # .groupby(['authorID_first', 'authorID']).size().reset_index(name='counts')
firstAuthorTmp = firstAuthorTmp[['authorID', 'paperID', 'authorOrder', 'authorID_first']].drop_duplicates()


print("compute first-author maps!", datetime.datetime.now().strftime("%H:%M:%S"))
# Perform the necessary merges and group by operations
merged_df = df_paper_author_field.merge(df_papers_field, on='paperID')
filtered_df = merged_df[merged_df['authorID'].isin(firstAuthorTmp['authorID_first'].unique())]

grouped = filtered_df.groupby(['authorID', 'authorOrder', 'year']).size().reset_index(name='cnt')

firstAuthorPaperCountMap = {}
firstAuthorWeightedPaperCountMap = {}

for _, row in grouped.iterrows():
    authorID = row['authorID'].strip()
    authorOrder = int(row['authorOrder'])
    year = int(row['year'])
    count = int(row['cnt'])

    # 更新firstAuthorPaperCountMap
    yearCountMap = firstAuthorPaperCountMap.setdefault(authorID, {})
    yearCountMap[year] = yearCountMap.get(year, 0) + count

    # 更新firstAuthorWeightedPaperCountMap
    if authorOrder <= MIN_STUDENT_AUTHOR_ORDER:
        yearWeightedCountMap = firstAuthorWeightedPaperCountMap.setdefault(authorID, {})
        yearWeightedCountMap[year] = yearWeightedCountMap.get(year, 0) + count / authorOrder


print("compute co-author maps!", datetime.datetime.now().strftime("%H:%M:%S"))
coauthor_joined = firstAuthorTmp.merge(
    df_paper_author_field[df_paper_author_field['authorOrder'] <= MIN_STUDENT_AUTHOR_ORDER], 
    left_on='authorID_first', right_on='authorID', suffixes=('', '_PA1')
).merge(
    df_paper_author_field, left_on=['authorID', 'paperID_PA1'], right_on=['authorID', 'paperID'], suffixes=('', '_PA2')
)

# Filtering based on the provided condition
coauthor_joined = coauthor_joined[coauthor_joined['authorOrder_PA1'] < coauthor_joined['authorOrder_PA2']]
# Joining with df_papers_field to get the year of each paper
coauthor_year_joined = coauthor_joined.merge(df_papers_field, on="paperID")
# Creating the coAuthorWeightedPaperCountMap and coAuthorPaperCountMap
grouped = coauthor_year_joined.groupby(['authorID_first', 'authorID', 'authorOrder_PA1', 'year']).size().reset_index(name='count')
grouped['coAuthorID'] = grouped['authorID_first'] + "-" + grouped['authorID']

coAuthorWeightedPaperCountMap = {}
coAuthorPaperCountMap = {}

for _, row in grouped.iterrows():
    coAuthorID = row['coAuthorID']
    year = row['year']
    count = row['count']
    authorOrder = row['authorOrder_PA1']
    
    # For coAuthorWeightedPaperCountMap
    yearWeightedCountMap = coAuthorWeightedPaperCountMap.setdefault(coAuthorID, {})
    yearWeightedCountMap[year] = yearWeightedCountMap.get(year, 0) + count / authorOrder

    # For coAuthorPaperCountMap
    yearCountMap = coAuthorPaperCountMap.setdefault(coAuthorID, {})
    yearCountMap[year] = yearCountMap.get(year, 0) + count


print("compute top-author maps!", datetime.datetime.now().strftime("%H:%M:%S"))
merged_df = df_paper_author_field_filtered.merge(df_papers_field, on='paperID', how='inner', suffixes=('', '_P'))
merged_df = merged_df[['authorID', 'paperID', 'authorOrder', 'year']].drop_duplicates()

# Group by necessary columns
grouped = filtered_df.groupby(['authorID', 'year']).size().reset_index(name='cnt')

topAuthorPaperCountMap = {}

for _, row in grouped.iterrows():
    authorID = row['authorID'].strip()
    year = int(row['year'])
    count = int(row['cnt'])

    # 更新topAuthorPaperCountMap
    yearCountMap = topAuthorPaperCountMap.setdefault(authorID, {})
    yearCountMap[year] = yearCountMap.get(year, 0) + count


# save all the maps to {path_to_csv}/*.json
with open(f"{path_to_csv}/firstAuthorPaperCountMap.json", "w") as f:
    json.dump(firstAuthorPaperCountMap, f)
with open(f"{path_to_csv}/firstAuthorWeightedPaperCountMap.json", "w") as f:
    json.dump(firstAuthorWeightedPaperCountMap, f)
with open(f"{path_to_csv}/coAuthorWeightedPaperCountMap.json", "w") as f:
    json.dump(coAuthorWeightedPaperCountMap, f)
with open(f"{path_to_csv}/coAuthorPaperCountMap.json", "w") as f:
    json.dump(coAuthorPaperCountMap, f)
with open(f"{path_to_csv}/topAuthorPaperCountMap.json", "w") as f:
    json.dump(topAuthorPaperCountMap, f)
