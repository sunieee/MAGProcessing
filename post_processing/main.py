import os
import pandas as pd
from utils import *


field = os.environ.get('field', 'visualization')
edge_df = pd.read_csv(f'out/{field}/edge_proba.csv')
node_dir = f'out/{field}'

df = pd.read_csv("links_jianchen0.csv", sep=',')
paper = pd.read_csv("0.csv", sep=',', index_col=0)
df = df[df["citingpaperID"].isin(paper["paperID"]) & df["citedpaperID"].isin(paper["paperID"])]
df = df.drop(columns=["sharedAuthor"])
df["extends_prob"] = 1
# df["citationContext"] = None
citingpaper = df["citingpaperID"].values.tolist()
citedpaper = df["citedpaperID"].values.tolist()
for p1, p2 in zip(citingpaper, citedpaper):
    cursor.execute(f"select citation_context from CitationContextContent where citingpaperID={p1} and citedpaperID={p2}")
    result = cursor.fetchone()
    if result:
        print(result[0])
    else:
        print(p1, p2)
df = df.reset_index(drop=True)
print(df)
df.to_csv("link_0.csv", sep=',')

df = pd.read_csv("papers_jianchen0.csv", sep=',')
paperID = df['paperID'].values.tolist()
sql = "select abstract from abstracts where paperID=%s"
abstract = []
for i in paperID:
    cursor.execute(sql, (str(i), ))
    result = cursor.fetchall()
    if result:
        abstract.append(result[0][0])
    else:
        abstract.append(None)
df["abstract"] = abstract
df.to_csv("update_papers_jianchen0.csv", sep=',')
    

if __name__ == '__main__':
    main()