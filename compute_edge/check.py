import pandas as pd
import numpy as np
import pickle
from sklearn.impute import SimpleImputer
import sys

file = open('saved_model_another_5depth.pickle', 'rb')
# file = open('../saved_model.pickle','rb')
model = pickle.load(file)

rank = sys.argv[1]

# df_links = pd.read_csv("/home/xfl/PyProject/visualization/reference/output/links_" + rank + ".csv", sep=',', index_col=0)
df_links = pd.read_csv("influence_arc_pushpakbhattacharyya1.csv", sep=',',
                       names=['childrenID', 'parentID', 'extendsProb', 'citationContext'])
print(df_links)
df_features = pd.read_csv("all_features.csv", sep=',')
# df_features = df_features[['citingpaperID', 'citedpaperID', "cross_correlation","window_cross_correlation","year_diff","citing_paper_citationcount","cited_paper_citationcount","self_cite","similarity","jaccard_cocitation","jaccard_bibcoupling"]]

not_in_df = df_links[~df_links.set_index(['childrenID', 'parentID']).index.isin(df_features.set_index(['citingpaperID', 'citedpaperID']).index)]
not_in_df = not_in_df.drop(columns=['extendsProb', 'citationContext'])
for column in df_features.columns:
    if column not in ['citingpaperID', 'citedpaperID']:
        not_in_df[column] = -1
not_in_df = not_in_df.rename(columns={'childrenID': 'citingpaperID', 'parentID': 'citedpaperID'})
print(not_in_df)

df_merge = df_features.merge(df_links, left_on=['citingpaperID', 'citedpaperID'], right_on=['childrenID', 'parentID'], how='inner')
df_features = df_merge[df_features.columns]
print(df_features)

df_features = pd.concat([df_features, not_in_df]).reset_index(drop=True)
print(df_features)

df_name = df_features[["citingpaperID", "citedpaperID"]]
df_features = df_features.drop(columns=["citingpaperID", "citedpaperID"])

numpy_array = df_features.to_numpy()
imp = SimpleImputer(missing_values=np.nan, strategy="constant", fill_value=-2)
numpy_array = imp.fit_transform(numpy_array)
db2 = pd.DataFrame(numpy_array)
db2=pd.concat([df_name,db2],axis=1)

edge = db2[['citingpaperID', 'citedpaperID']]
db2 = db2.drop(columns=['citingpaperID', 'citedpaperID'])
result = model.predict_proba(db2)[:, 1]

result = pd.DataFrame(result, columns=['proba'])
print(result)
edge = pd.concat([edge, result], axis=1)
print(edge)
edge.to_csv('edge_prob_result.csv', index=None)
