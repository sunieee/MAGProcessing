from gensim import utils
import re
import gensim
from gensim.parsing.preprocessing import preprocess_string
gensim.parsing.preprocessing.STOPWORDS = set()
import time

def strip_short2(s, minsize=2):
    s = utils.to_unicode(s)
    def remove_short_tokens(tokens, minsize):
        return [token for token in tokens if len(token) >= minsize]
    return " ".join(remove_short_tokens(s.split(), minsize))

gensim.parsing.preprocessing.DEFAULT_FILTERS[6]=strip_short2
del gensim.parsing.preprocessing.DEFAULT_FILTERS[-1]
# ACM_Autho_set=set(preprocess_string(re.sub('[^\s\w]', "", ACM_Author_str)))

dict={}
dict['scigene_ComputerGraphicsImages_field'] = [1,11,17]
dict['scigene_ComputerSecurity_field'] = [1,12]
dict['scigene_DataMining_field'] = [1,13,14]
dict['scigene_HCI_field'] = [1,15,16]
dict['scigene_ProgrammingLanguage_field'] = [1,18]
dict['scigene_SoftwareEngineering_field'] = [1,5]
dict['scigene_database_field'] = [1,4]
dict['scigene_SpeechRecognition_field']= [1,6,7,8]
dict['scigene_TheoreticalComputerScience_field'] =[1,9]
dict['scigene_NLP_field']=[1,2,3]
dict['scigene_physics_field']=[19,20]


import pandas as pd
import numpy as np
import sys
from sqlalchemy import create_engine

if len(sys.argv) < 2:
    print("Not enough parameters: ", len(sys.argv))
    sys.exit

field_ = sys.argv[1]

from pymysql import connect
import pymysql

connection = pymysql.connect(host='localhost',
                                     port=3306,
                                     user='root',
                                     password='Vis_2014',
                                     db=field_,
                                     charset='utf8')

cursor = connection.cursor()
sql='update authors_field set FellowType=NULL;'
print(sql)
cursor.execute(sql)
connection.commit()

engine = create_engine('mysql+pymysql://root:Vis_2014@localhost:3306/scigene_CareerAward')
sql_data = '''select * from award_authors;'''
db_data = pd.read_sql_query(sql_data, engine)

engine_MAG = create_engine('mysql+pymysql://root:Vis_2014@localhost:3306/'+field_)
sql_data = '''select * from authors_field;'''
author_data = pd.read_sql_query(sql_data, engine_MAG)
author_with_label = author_data[['name','FellowType']]

fellow_author=pd.DataFrame()
for i in range(len(dict[field_])):
    temp=db_data[db_data['type']==dict[field_][i]]
    fellow_author=pd.concat([fellow_author,temp])

# fellow_author=fellow_author['original_author_name'].values

for i in range(author_data.shape[0]):
    name=author_data.iloc[i][2]
    name=set(preprocess_string(name))
    for j in range(fellow_author.shape[0]):
        f_name=fellow_author.iloc[j][0]
        f_name_temp=f_name
        f_name=set(preprocess_string(f_name))
        x = f_name.intersection(name)
        if(len(x)>=2 or (len(x)==1 and len(name)==1 and len(f_name_temp)==1)):
            print(x,author_data.iloc[i][2],fellow_author.iloc[j][0])
            if(author_with_label.loc[i]['FellowType']==None):
                author_with_label.loc[i]['FellowType']=str(fellow_author.iloc[j][2])+':'+str(fellow_author.iloc[j][1])+','
            else:
                author_with_label.loc[i]['FellowType']+=str(fellow_author.iloc[j][2])+':'+str(fellow_author.iloc[j][1])+','
            print(author_with_label.loc[i]['FellowType'])
            # aim_index = np.where(fellow_author==fellow_author[j])
            # fellow_author=np.delete(fellow_author,aim_index)
            # time.sleep(2)
pd.set_option('display.max_rows',None)
pd.set_option('display.max_columns',None)
author_with_label=author_with_label.dropna()
print(author_with_label,author_with_label.shape)


for i in range(author_with_label.shape[0]):
    sql='update authors_field set FellowType=\''+author_with_label.iloc[i][1]+'\' where name=\''+author_with_label.iloc[i][0]+'\';'
    print(sql)
    cursor.execute(sql)
    connection.commit()

# sql='update authors_field set FellowType=NULL where authorRank>1000;'
# print(sql)
# cursor.execute(sql)
# connection.commit()

# SET @Rank = 0;
# SELECT @Rank:=@Rank + 1 rank
#     FROM authors_field
# ORDER BY hIndex_field DESC;