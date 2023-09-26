import numpy as np
import os
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score,precision_recall_curve
from sklearn.metrics import precision_score, recall_score, f1_score
import pickle

file = open('out/saved_model_another_5depth.pickle','rb')
model = pickle.load(file)

df_test = pd.read_csv('out/all_features.csv')
# y=df_test["cite_function"]
# y=y.replace("Extends",0)
# y=y.replace("Others",1)
# print(y)
df_name=df_test[["citingpaperID","citedpaperID"]]
df_test=df_test.drop(columns=["citingpaperID","citedpaperID"])

# print(df_test.describe())
numpy_array = df_test.to_numpy()
imp = SimpleImputer(missing_values=np.nan, strategy="constant", fill_value=-2)
numpy_array = imp.fit_transform(numpy_array)
db2=pd.DataFrame(numpy_array)

db2=pd.concat([df_name,db2],axis=1)
print(db2)

edge=db2[['citingpaperID','citedpaperID']]
db2=db2.drop(columns=['citingpaperID','citedpaperID'])
result=model.predict_proba(db2)[:,1]
# result=model.predict(db2)
# result=pd.DataFrame(result)
# result=result.replace("Extends",0)
# result=result.replace("Others",1)
# print(result)
# print(precision_score(y,result), recall_score(y,result), f1_score(y,result))

result=pd.DataFrame(result,columns=['proba'])
print(result)
edge=pd.concat([edge,result],axis=1)
print(edge)
edge.to_csv('out/edge_proba_depth5_36features.csv',index=None)
