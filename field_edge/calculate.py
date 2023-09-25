import pandas as pd
import numpy as np

def cos_sim(a, b):
    a_norm = np.linalg.norm(a)
    b_norm = np.linalg.norm(b)
    cos = np.dot(a,b)/(a_norm * b_norm)
    return cos

df_csv=pd.read_csv("similarity_features.csv")
print(df_csv)
df_csv.pop('title')
df_csv.pop('abstract')
print(df_csv)
arc_new=[]
arc=pd.read_csv('all_features.csv')
# arc=df
for i in range(arc.shape[0]):
    citing=(int(arc.iloc[i]['citingpaperID']))
    cited=(int(arc.iloc[i]['citedpaperID']))
    
    db_citing=df_csv.loc[df_csv['paperID']==citing]
    db_cited=df_csv.loc[df_csv['paperID']==cited]

    # print(citing,db_citing)
    if(len(db_citing.values)==0 or len(db_cited.values)==0):
        arc_new.append(np.nan)
    else:
        vector_citing=db_citing.iloc[0][1:]
        vector_cited=db_cited.iloc[0][1:]
        similarity=cos_sim(vector_citing,vector_cited)
        print(similarity)
        arc_new.append(similarity)
print(len(arc_new))
df_new = pd.DataFrame(arc_new, columns=['similarity'])
df_csv=pd.read_csv("all_features.csv")
# df_csv=df_csv.drop(columns=['similarity'])
df_feature=pd.concat([df_csv, df_new], axis=1)
# df_feature=pd.concat([df, df_new], axis=1)
df_feature.to_csv('all_features.csv',index=False)

