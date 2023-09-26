import numpy as np
import pickle
from sklearn.model_selection import cross_val_score, KFold
from sklearn.metrics import f1_score
import mysql.connector

modelPath = "saved_model_another_5depth.pickle"

host = "192.168.0.140"
user = "root"
password = "root"
database = "scigene_link_labels"
sql = "select * from final_dataset_link_label_with_combined_features;"

conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
cursor = conn.cursor()

cursor.execute(sql)
result = cursor.fetchall()
print(len(result))

cursor.close()
conn.close()

# file = open(modelPath, 'rb')
# model = pickle.load(file)

# kf = KFold(n_splits=10, shuffle=True, random_state=42)  # 10折交叉验证，可以根据需要调整参数

# f1_scores = cross_val_score(model, X, y, cv=kf, scoring='f1_macro')  # 使用'f1_macro'来计算多类别F1-score
# mean_f1_score = np.mean(f1_scores)
# std_f1_score = np.std(f1_scores)

# print("Mean F1-score:", mean_f1_score)
# print("Standard Deviation of F1-score:", std_f1_score)
