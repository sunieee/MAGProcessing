# 将某领域的所有论文构成的topic匹配到top1000统计的Count-Name中，里面有一些trick
import pandas as pd
import json

df_topic_count_name = pd.read_csv("./vis_output2/topic-word.csv", sep=',')
df_topic_count_name['Count'] = 0
topic_count_name = df_topic_count_name.values.tolist()
for i in range(1, 1001):
    df = pd.read_csv("./csv2/papers_" + str(i) + ".csv", sep=',', index_col=0)
    papers = df.values.tolist()
    for paper in papers:
        topic_count_name[paper[9]][1] += 1
df = pd.DataFrame(topic_count_name, columns=["Topic", "Count", "Word(Proba)"])
df = df.sort_values(by="Count", ascending=False)
df = pd.concat([df.iloc[3:4], df.iloc[:3], df.iloc[4:]])
df["Topic"] = [i for i in range(df.shape[0])]
df.to_csv("top1000.csv", index=False)
