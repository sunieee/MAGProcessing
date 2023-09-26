import pandas as pd
import os

directory = "./csv/"
new_directory = "./sort_csv/"

df_raw = pd.read_csv("top_field_authors_update.csv", sep=',',
                     names=["authorID", "rank", "name", "PaperCount", "CitationCount", "PaperCount_field", "authorRank", "CitationCount_field", "hIndex_field", "FellowType"])
df_match = pd.read_csv("match.csv", sep=',', index_col=0)

df_reserve = df_raw[~df_raw["authorRank"].isin(df_match['r2'])]
df_sort = df_reserve.sort_values(by="PaperCount_field", ascending=False)
df_sort = df_sort.reset_index(drop=True)

for index, row in df_sort.iterrows():
    old_idx = row["authorRank"]
    new_idx = index + 1
    if old_idx == 148:
        print(old_idx, new_idx)
    os.rename(os.path.join(directory, "papers_" + str(old_idx) + ".csv"), os.path.join(new_directory, "papers_" + str(new_idx) + ".csv"))
    os.rename(os.path.join(directory, "links_" + str(old_idx) + ".csv"), os.path.join(new_directory, "links_" + str(new_idx) + ".csv"))
df_sort["authorRank"] = range(1, len(df_sort) + 1)
df_sort.to_csv("sort_top_field_authors.csv", sep=',', index=False, header=False)
