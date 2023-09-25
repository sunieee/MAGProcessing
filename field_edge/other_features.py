# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sys
if len(sys.argv) < 2:
    print("Not enough parameters: ", len(sys.argv))
    sys.exit

database = sys.argv[1]

engine = create_engine('mysql+pymysql://root:Vis_2014@localhost:3306/'+database)

engine4 = create_engine('mysql+pymysql://root:Vis_2014@localhost:3306/'+database)

if database=="scigene_acl_anthology":
    sql = '''select * from papers_ARC;'''
    sql4 = '''select * from paper_author_ARC;'''
else:
    sql = '''select * from papers_field;'''
    sql4 = '''select * from paper_author_field;'''

db = pd.read_sql_query(sql, engine)
db4 = pd.read_sql_query(sql4, engine4)

import numpy as np
import pandas as pd
import os
import re
arc=pd.read_csv('features.csv')
# arc=df
arc_new=[]
for i in range(arc.shape[0]):
    citing=str(int(arc.iloc[i]['citingpaperID']))
    cited=str(int(arc.iloc[i]['citedpaperID']))
    
    arc_new_features=[]

    db_citing=db.loc[db['paperID']==citing]
    db_cited=db.loc[db['paperID']==cited]
    # print(db_citing,db_cited)
    if(len(db_citing.values)==0 or len(db_cited.values)==0):
        arc_new_features.append(np.nan)
    else:
        year_citing=int(db_citing['year'].values[0])
        year_cited=int(db_cited['year'].values[0])
        if(year_citing-year_cited>=0):
            arc_new_features.append(year_citing-year_cited)
        else:
            arc_new_features.append(np.nan)
    
    if(len(db_citing.values)==0):
        arc_new_features.append(np.nan)
    else:
        num_citing=db_citing['citationCount'].values[0]
        if(num_citing==-1):
            num_citing=np.nan
        arc_new_features.append(num_citing)
    
    if(len(db_cited.values)==0):
        arc_new_features.append(np.nan)
    else:
        num_cited=db_cited['citationCount'].values[0]
        if(num_cited==-1):
            num_cited=np.nan
        arc_new_features.append(num_cited)

    citing_authors=db4.loc[db4['paperID']==citing]['authorID'].values.tolist()
    cited_authors=db4.loc[db4['paperID']==cited]['authorID'].values.tolist()
    print(citing,citing_authors,cited,cited_authors)
    if(len(citing_authors)==0 or len(cited_authors)==0):
        arc_new_features.append(np.nan)
    else:
        self_cite=set(citing_authors).intersection(set(cited_authors))
        if(len(self_cite)==0):
            arc_new_features.append(0)
        else:
            arc_new_features.append(len(self_cite))
    arc_new.append(arc_new_features)
    print(i,arc_new_features)

df_new = pd.DataFrame(arc_new, columns=['year_difference', 'citingpaperCitationCount', 'citedpaperCitationCount', 'self_cite'])
print(df_new.isna().sum())
df_csv=pd.read_csv("features.csv")
df_feature=pd.concat([df_csv, df_new], axis=1)
# df_feature=pd.concat([df, df_new], axis=1)
df_feature.to_csv('all_features.csv',index=False)