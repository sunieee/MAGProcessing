# data = [
#     [0, 21, 46, 2, 9, 16, 26, 27, 22, 24, 38, 52],
#     []
# ]
from sklearn.cluster import KMeans
import pandas as pd
import sys

if len(sys.argv) != 2:
    print("Error: python clusterTopic.py $num")
    sys.exit(1)

numClusters = int(sys.argv[1])
path = "./vis_output2/"

df = pd.read_csv(path + "field.csv", sep=',')
X = df[['x', 'y']].values.tolist()
y = [0 for _ in range(len(X))]
clf = KMeans(n_clusters=numClusters)     # 聚类数量
clf.fit(X, y)
print(clf.labels_)

df_label = pd.DataFrame(clf.labels_, columns=["label"])
df_merged = pd.concat([df, df_label], axis=1)
df_merged.to_csv(path + "field_leaves.csv", sep=',', index=False)

from matplotlib import pyplot as plt
a = [n[0] for n in X]  
b = [n[1] for n in X]
plt.scatter(a, b, c=clf.labels_)
plt.savefig(path + "topic.png")