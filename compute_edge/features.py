import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import sys

engine3 = create_engine('mysql+pymysql://root:Vis_2014@localhost:3306/scigene_features_new')
sql3 = '''select * from all_ARC_ARC_links_with_combined_features;'''
db3 = pd.read_sql_query(sql3, engine3)

db3.to_csv('all_features.csv',index=None)