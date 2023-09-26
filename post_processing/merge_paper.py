import pandas as pd

df_author = pd.read_csv("csv/top_field_authors.csv", sep=',',
                        names=["authorID", "rank", "name", "PaperCount", "CitationCount", "PaperCount_field", "authorRank", "CitationCount_field", "hIndex_field", "FellowType"])
prev_authorRank = None
for index, row in df_author.iterrows():
    curr_authorRank = row['authorRank']
    if prev_authorRank is not None and curr_authorRank != prev_authorRank + 1:
        print(f"error authorRank {curr_authorRank} {prev_authorRank}")
        curr_authorRank = prev_authorRank + 1
        df_author.at[index, "authorRank"] = curr_authorRank
    prev_authorRank = curr_authorRank

df_match = pd.read_csv("match.csv", sep=',', index_col=0)
R1 = df_match['r1'].values.tolist()
R2 = df_match['r2'].values.tolist()

for r1, r2 in zip(R1, R2):
    paper1 = "./csv/papers_" + str(r1) + ".csv"
    paper2 = "./csv/papers_" + str(r2) + ".csv"
    df1 = pd.read_csv(paper1, sep=',', index_col=0)
    df2 = pd.read_csv(paper2, sep=',', index_col=0)
    df_match = df1[df1["paperID"].isin(df2["paperID"])]
    df_merged = pd.concat([df1, df2])
    df_merged = df_merged.reset_index(drop=True)
    print("node:")
    print(len(df1), len(df2), len(df_merged))
    df_merged.to_csv("./csv/papers_" + str(r1) + ".csv", sep=',')
    df_author.at[r1 - 1, "PaperCount_field"] = len(df_merged)
    df_author.at[r1 - 1, "CitationCount_field"] = df_author.at[r1 - 1, "CitationCount_field"] + df_author.at[r2 - 1, "CitationCount_field"]
    df_author.at[r1 - 1, "hIndex_field"] = df_author.at[r1 - 1, "hIndex_field"] + df_author.at[r2 - 1, "hIndex_field"]
    
    link1 = "./csv/links_" + str(r1) + ".csv"
    link2 = "./csv/links_" + str(r2) + ".csv"
    df1 = pd.read_csv(link1, sep=',', index_col=0)
    df2 = pd.read_csv(link2, sep=',', index_col=0)
    # df_match = df1[df1["paperID"].isin(df2["paperID"])]
    df_match = df1[df1.set_index(['childrenID', 'parentID']).index.isin(df2.set_index(['childrenID', 'parentID']).index)]
    df_merged = pd.concat([df1, df2])
    df_merged = df_merged.reset_index(drop=True)
    print("edge:")
    print(len(df1), len(df2), len(df_merged))
    df_merged.to_csv("./csv/links_" + str(r1) + ".csv", sep=',')

# df_sort = df_author.sort_values(by="PaperCount_field", ascending=False)
# df_sort = df_sort.reset_index(drop=True)
# df_sort.to_csv("sort_top_field_authors.csv", sep=',')
# print(df_sort[["PaperCount_field", "authorRank"]])
df_author.to_csv("top_field_authors_update.csv", sep=',', index=False, header=False)