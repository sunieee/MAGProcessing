# data = [
#     [0, 21, 46, 2, 9, 16, 26, 27, 22, 24, 38, 52],
#     []
# ]
from sklearn.cluster import KMeans
import pandas as pd
import os
import sys
if len(sys.argv) != 3:
    print("Error: python clusterTopic.py $field $num")
    sys.exit()
field = sys.argv[1]
directory = sys.path[0] + "/output/" + field
numClusters = int(sys.argv[2])

df = pd.read_csv(os.path.join(directory, "field.csv"), sep=',')
X = df[['x', 'y']].values.tolist()
y = [0 for _ in range(len(X))]
clf = KMeans(n_clusters=numClusters)     # 聚类数量
clf.fit(X, y)
print(clf.labels_)

df_label = pd.DataFrame(clf.labels_, columns=["label"])
df_merged = pd.concat([df, df_label], axis=1)
df_merged.to_csv(os.path.join(directory, "field_leaves.csv"), sep=',', index=False)

from matplotlib import pyplot as plt
a = [n[0] for n in X]  
b = [n[1] for n in X]
plt.scatter(a, b, c=clf.labels_)
plt.savefig(os.path.join(directory, "topic.png"))