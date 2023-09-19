import pandas as pd
import numpy as np

df_authors = pd.read_csv("authors_MAG.csv")
df_paper_author = pd.read_csv("paper_author_MAG.csv")
print(len(df_paper_author))
df_temp = df_paper_author[~df_paper_author['authorID'].isin(df_authors['authorID'])]
print(len(df_temp))
df_paper_author = df_paper_author[~df_paper_author['paperID'].isin(df_temp['paperID'])]
# df_paper_author = df_paper_author[df_paper_author['authorID'].isin(df_authors['authorID'])].dropna()
print(len(df_paper_author))
# df_paper_author.to_csv("paper_author_MAG.csv", index=False)

