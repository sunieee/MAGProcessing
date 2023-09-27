import csv
import re
import pandas as pd
import gensim
import nltk
from nltk.corpus import stopwords
import ssl
import numpy as np
from gensim import utils
from utils import *

# run before
# nltk.download('stopwords')
x_train = []

############################################################
# 实际上，你只需要训练Doc2Vec模型一次。
# 一旦模型被训练，你可以多次使用它来推断新的文档向量，而不需要重新训练。
# RETRAIN_GENSIM = False 时，加载已经训练好的模型，并使用它来推断新的文档向量
############################################################
RETRAIN_GENSIM = False

# read all the selected papers in MAG
db_suffix = 'ARC' if database=="scigene_acl_anthology" else 'field'
db = pd.read_sql_query(f'''select paperID, title, abstract 
                        from papers_{db_suffix}
                        where paperID in {nodes}''', engine)

print('similarity_mysql:',len(db))
print(db.head())

# Drop the first column and convert to list
result = db.iloc[:, 1:].values.tolist()

# Append the first value of each row twice
result = [row + [row[0], row[0]] for row in result]

# print(result[:5])

# Tokenize and remove stopwords
stoplist = set(stopwords.words('english'))
def tokenize(text):
    return [word for word in utils.tokenize(text.lower()) if word not in stoplist]
result = [tokenize(' '.join([str(item) if item else '' for item in row]).lower()) for row in result]

# Create TaggedDocument for training
x_train = [gensim.models.doc2vec.TaggedDocument(words=row, tags=[i]) for i, row in enumerate(result)]

# Train the Doc2Vec model
if RETRAIN_GENSIM:
    model = gensim.models.doc2vec.Doc2Vec(vector_size=2, min_count=5, epochs=20)
    model.build_vocab(x_train)
    model.train(x_train, total_examples=model.corpus_count, epochs=model.epochs)
    model.save("model1.txt")
else:
    # Load the trained model
    model = gensim.models.doc2vec.Doc2Vec.load("model1.txt")

# Infer vectors for each document
output = [model.infer_vector(doc.words) for doc in x_train]

# Create a DataFrame with the features
df_feature = pd.DataFrame(output)
df_feature = pd.concat([db, df_feature], axis=1)

# Drop duplicates and rows with 'paperID' as 'paperID'
df_feature = df_feature.drop_duplicates()
df_feature = df_feature[df_feature['paperID'] != 'paperID']

# Save to CSV
df_feature.to_csv('out/similarity_features.csv', index=False)
print('similarity_features saved', len(df_feature))
print(df_feature.head())
