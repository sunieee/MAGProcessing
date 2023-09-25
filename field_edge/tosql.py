import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
from utils import *

df_csv=pd.read_csv("all_features.csv")

# df=pd.read_csv("features.csv")
# print(df,df_csv)
# df=df['cross_correlation_feature']
# df_csv=pd.concat([df,df_csv],axis=1)
# print(df_csv)
# df_csv=df_csv[['citingpaperID','citedpaperID','cross_correlation_feature','negativetimelagged_cross_correlation_feature','timelagged_cross_correlation_feature','window_cross_correlation_feature','window_negativetimelagged_cross_correlation_feature','window_timelagged_cross_correlation_feature','year_difference','citingpaperCitationCount','citedpaperCitationCount','self_cite','similarity','raw_cocitation','cosine_cocitation','jaccard_cocitation','raw_bibcoupling','cosine_bibcoupling','jaccard_bibcoupling']]
# print(df_csv)
# df_csv.to_csv('all_features.csv',index=False)


df_csv.to_sql('all_dataset_link_with_features', engine, if_exists='replace', index=False, dtype={
        "citingpaperID": sqlalchemy.types.NVARCHAR(length=100),
        "citedpaperID": sqlalchemy.types.NVARCHAR(length=100),
        "year_difference":sqlalchemy.types.INTEGER(), 
        "citingpaperCitationCount":sqlalchemy.types.INTEGER(),
        "citedpaperCitationCount":sqlalchemy.types.INTEGER(),
        "self_cite":sqlalchemy.types.INTEGER(),
        "raw_cocitation": sqlalchemy.types.INTEGER(),
        "raw_bibcoupling": sqlalchemy.types.INTEGER()
    })
