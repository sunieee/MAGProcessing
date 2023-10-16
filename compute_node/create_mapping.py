from utils import *
import json
import datetime
import pandas as pd

MIN_STUDENT_AUTHOR_ORDER = 3

# Load CSV files into DataFrames
path_to_csv = f"out/{database}/csv"

print('loading data from database', datetime.datetime.now().strftime("%H:%M:%S"))
if not os.path.exists(path_to_csv):
    os.makedirs(path_to_csv)
    df_paper_author_field = pd.read_sql_query(f"select * from paper_author_field", conn)
    df_paper_author_field.to_csv(f"{path_to_csv}/paper_author_field.csv", index=False)
    
    df_papers_field = pd.read_sql_query(f"select * from papers_field", conn)
    df_papers_field.to_csv(f"{path_to_csv}/papers_field.csv", index=False)
else:
    df_paper_author_field = pd.read_csv(f"{path_to_csv}/paper_author_field.csv")
    df_papers_field = pd.read_csv(f"{path_to_csv}/papers_field.csv")

df_paper_author_field['authorID'] = df_paper_author_field['authorID'].astype(str)
df_paper_author_field['paperID'] = df_paper_author_field['paperID'].astype(str)
df_papers_field['paperID'] = df_papers_field['paperID'].astype(str)
# df_paper_author_field.set_index('paperID', inplace=True)
# df_papers_field.set_index('paperID', inplace=True)

top_field_paper_author_df = top_field_authors_df.merge(df_paper_author_field, on="authorID")
top_field_paper_author_df = top_field_paper_author_df[['authorID', 'paperID', 'authorOrder']].drop_duplicates()

# Creating the firstAuthorTmp DataFrame
print("Pre-compute first author maps!", datetime.datetime.now().strftime("%H:%M:%S"))
firstAuthorTmp = top_field_paper_author_df.merge(df_paper_author_field, on="paperID", suffixes=('', '_first')) \
    .query("authorOrder > 1 and authorOrder_first == 1") 
    # .groupby(['authorID_first', 'authorID']).size().reset_index(name='counts')
firstAuthorTmp = firstAuthorTmp[['authorID', 'paperID', 'authorOrder', 'authorID_first']].drop_duplicates()

print("compute first-author maps!", datetime.datetime.now().strftime("%H:%M:%S"))
firstAuthorPapers = df_papers_field.merge(df_paper_author_field, on="paperID") \
    .loc[df_paper_author_field['authorID'].isin(firstAuthorTmp['authorID_first'].unique()), :]
firstAuthorPaperCountMap = firstAuthorPapers.groupby(['authorID', 'year', 'authorOrder']).size().to_dict()

print("compute top-author maps!", datetime.datetime.now().strftime("%H:%M:%S"))
filtered_authors_papers = top_field_paper_author_df.merge(df_papers_field, on="paperID")
topAuthorPaperCountMap = filtered_authors_papers.groupby(['authorID', 'year']).size().to_dict()

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
