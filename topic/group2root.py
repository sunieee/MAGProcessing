import pandas as pd
import sys

if len(sys.argv) != 2:
    print("Error: python group2root.py $num")
    sys.exit(1)

numClusters = int(sys.argv[1])
path = "./vis_output2/"

df = pd.read_csv(path + "field_leaves.csv", sep=',')
roots = {}
lst = [[] for _ in range(numClusters)]   # 聚类数目

for i, row in df.iterrows():
    lst[row["label"]].append([row["Topic"], row["Name"], row["Count"]])
    if row["label"] not in roots.keys():
        roots[row["label"]] = [row["label"], row["Count"], row["label"], row['x'], row['y'], row['h'], row['s'], row['v']]
    else:
        roots[row["label"]][1] += row["Count"]
roots = sorted(roots.values(), key=lambda x:x[0])
df = pd.DataFrame(roots, columns=["Topic", "Count", "Name", 'x', 'y', 'h', 's', 'v'])
df.to_csv(path + "field_roots.csv", sep=',', index=False)

for i in range(len(lst)):
    print("group", i)
    for topic in lst[i]:
        print(int(topic[0]), topic[1], round(topic[2] / len(lst[i]), 1))
    print()
