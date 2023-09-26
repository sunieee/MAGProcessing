import csv
import re
import pandas as pd
import gensim
import nltk
from nltk.corpus import stopwords
import numpy as np
import math
import ssl
import random
import collections
from gensim import utils
# def cos_sim(a, b):
#     a_norm = np.linalg.norm(a)
#     b_norm = np.linalg.norm(b)
#     cos = np.dot(a,b)/(a_norm * b_norm)
#     return cos

x_train = []

reader = csv.reader(open("similarity_mysql.csv",'rt',encoding='utf-8'))
result = list(reader)
result.pop(0)

# result_init=result.copy()

# for i in range(579304//70000):
#     result = np.array(result_init[70000*i:70000*(i+1)])
#     temp = result
#     result = result[:,1:]
#     result = result.tolist()
#     for i in range(len(result)):
#         result[i].append(result[i][0])
#         result[i].append(result[i][0])
#     stoplist = set(stopwords.words('english'))
#     result = [[word for word in utils.tokenize(' '.join(result[i]).lower()) if word not in stoplist] for i in range(len(result))]
#     for i,text in enumerate(result):
#         l = len(text)
#         if(l!=0):
#             text[l-1] = text[l-1].strip()
#         document = gensim.models.doc2vec.TaggedDocument(text,tags=[i])
#         # print(document)
#         x_train.append(document)

#     model = gensim.models.doc2vec.Doc2Vec.load("model1.txt")

#     output = []

#     for i in range(temp.shape[0]):
#         test = model.infer_vector(x_train[i].words)
#         output.append(test)
#         if(i==0):
#             print(test)
#             print(x_train[i].words)


#     df_feature = pd.DataFrame(output)
#     df_csv=pd.DataFrame(temp,columns=['paperID', 'title', 'abstract'])
#     df_feature=pd.concat([df_csv, df_feature], axis=1)
#     print(df_feature)
#     df_feature.to_csv('similarity_features.csv',index=False,mode='a+')

# i=(579304//70000)
# result = np.array(result_init[70000*i:])

result = np.array(result)
temp = result
result = result[:,1:]
result = result.tolist()
for i in range(len(result)):
    result[i].append(result[i][0])
    result[i].append(result[i][0])
stoplist = set(stopwords.words('english'))
result = [[word for word in utils.tokenize(' '.join(result[i]).lower()) if word not in stoplist] for i in range(len(result))]
for i,text in enumerate(result):
    l = len(text)
    if(l!=0):
        text[l-1] = text[l-1].strip()
    document = gensim.models.doc2vec.TaggedDocument(text,tags=[i])
    x_train.append(document)

model = gensim.models.doc2vec.Doc2Vec.load("model1.txt")

output = []

for i in range(temp.shape[0]):
    test = model.infer_vector(x_train[i].words)
    output.append(test)
    if(i==0):
        print(test)
        print(x_train[i].words)


df_feature = pd.DataFrame(output)
df_csv=pd.DataFrame(temp,columns=['paperID', 'title', 'abstract'])
df_feature=pd.concat([df_csv, df_feature], axis=1)
print(df_feature)
df_feature=df_feature.drop_duplicates()
df_feature=df_feature[df_feature['paperID']!='paperID']
# df_feature.to_csv('similarity_features.csv',index=False,mode='a+')
df_feature.to_csv('similarity_features.csv',index=False)