import pandas as pd
from utils import *


sql_paper = '''select * from all_dataset_link_with_features; '''
db_data=pd.read_sql_query(sql_paper, engine)
db_data['citingpaperID']=db_data['citingpaperID'].astype(str)
db_data['citedpaperID']=db_data['citedpaperID'].astype(str)
db_data.to_csv('all_features.csv',index=False)