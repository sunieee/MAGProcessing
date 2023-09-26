import pandas as pd
import os

directory = "csv/"
new_directory = "sort_csv/"
df_author = pd.read_csv("sort_top_field_authors.csv", sep=',', index_col=0)
lst = [6, 31, 78, 148, 149, 611]
for index, row in df_author.iterrows():
    old_idx = row["authorRank"]
    new_idx = index + 1
    if new_idx == 144:
        print(old_idx, new_idx)
    os.rename(os.path.join(directory, "papers_" + str(old_idx) + ".csv"), os.path.join(new_directory, "papers_" + str(new_idx) + ".csv"))
    os.rename(os.path.join(directory, "links_" + str(old_idx) + ".csv"), os.path.join(new_directory, "links_" + str(new_idx) + ".csv"))
df_author["authorRank"] = range(1, len(df_author) + 1)
df_author.to_csv("sort_top_field_authors.csv", sep=',')