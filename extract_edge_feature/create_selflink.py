import os
import re
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import sys

if len(sys.argv) < 2:
    print("Not enough parameters: ", len(sys.argv))
    sys.exit

hI=''
if len(sys.argv) > 2:
    tmp=str(sys.argv[2]).strip().lower()
    if tmp=='hi':
        hI+='_hI'
        print("hI rank mode on with argv[2]:",tmp)
    else:
        print("hI rank mode off with argv[2]:",tmp)
        sys.exit
        

database = sys.argv[1]
engine = create_engine('mysql+pymysql://root:Vis_2014@localhost:3306/'+database)
database=database.split('_')[1]+hI
print('create selflinks from',database)
flag=0
print(database)
for root, dirs, files in os.walk("../parser/data/csv/"+database+"/", topdown=False):
        for name in files:
            if(name[0]=='n'):
                if flag==0:
                    df=pd.read_csv("../parser/data/csv/"+database+"/"+name,header=None, sep='\t')
                    flag=1
                else:
                    try:
                        df_temp=pd.read_csv("../parser/data/csv/"+database+"/"+name, header=None, sep='\t')
                        df = pd.concat([df,df_temp])
                    except:
                        print(name)
print(df)
df=df[[0,1]]
df.columns = ["citingpaperID", "citedpaperID"]

df.to_sql('all_dataset_link', engine, if_exists='replace', index=False, dtype={"citingpaperID": sqlalchemy.types.NVARCHAR(length=100),\
    "citedpaperID": sqlalchemy.types.NVARCHAR(length=100)})

