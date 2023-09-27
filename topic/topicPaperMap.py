# 查找某个topic对应在前1000名scholar中的论文信息
import pandas as pd
import sys

if len(sys.argv) != 2:
    print("Error: python topicPaperMap.py topicID")
    sys.exit(1)

topicId = int(sys.argv[1])
path = "./csv2/"

res = pd.DataFrame()
for i in range(1, 1001):
    df = pd.read_csv(path + "papers_" + str(i) + ".csv", sep=',', index_col=0)
    df = df[df['topic'] == topicId]
    res = pd.concat([res, df])

res = res.reset_index(drop=True)
res.to_csv("topic" + sys.argv[1] + "-papers.csv", sep=',')