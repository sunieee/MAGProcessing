import csv
import re
import pandas as pd
import gensim
import nltk
from nltk.corpus import stopwords
import ssl
import numpy as np
from gensim import utils

nltk.download('stopwords')
x_train = []

reader = csv.reader(open("similarity_mysql.csv",'rt',encoding='utf-8'))
result = list(reader)
result.pop(0)
result = np.array(result)
print(result.shape)
result = result[:,1:]
result = result.tolist()
for i in range(len(result)):
    result[i].append(result[i][0])
    result[i].append(result[i][0])
stoplist = set(stopwords.words('english'))
result = [[word for word in utils.tokenize(' '.join(result[i]).lower()) if word not in stoplist] for i in range(len(result))]

for i,text in enumerate(result):
    l = len(text)
    text[l-1] = text[l-1].strip()
    document = gensim.models.doc2vec.TaggedDocument(text,tags=[i])
    x_train.append(document)

model = gensim.models.doc2vec.Doc2Vec(vector_size=2,min_count = 5,epochs=20)
model.build_vocab(x_train)
model.train(x_train,total_examples=model.corpus_count,epochs=model.epochs)
model.save("model1.txt")
#model = gensim.models.doc2vec.Doc2Vec.load("C:/Users/alienware/Desktop/model1.txt")
#vector = model.infer_vector(["system", "response"])
#list_of_vec = []
#for i in range(1000):
#    list_of_vec.append(model.docvecs[i])
#test = pd.DataFrame(data = list_of_vec)
#test.to_csv('C:/Users/alienware/Desktop/model_vec1.csv',index= False, encoding='utf-8')
#print(vector)
