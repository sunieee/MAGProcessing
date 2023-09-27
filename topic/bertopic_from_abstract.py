import pandas as pd
import numpy as np
import json
from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import MinMaxScaler
from umap import UMAP
import mysql.connector
import sys
import os

if len(sys.argv) != 2:
    print("Error: python bertopic.py 1/2")
    sys.exit(1)

paperKey = []
docs = []

# 方法1，提取所有该领域论文
# TODO 不同领域数据库需要修改
if sys.argv[1] == '1':
    conn = mysql.connector.connect(host="192.168.0.140",
                                user="root",
                                password="root",
                                database="scigene_visualization_field")
    cursor = conn.cursor()
    sql = "select paperID, title, abstract from papers_field" # TODO 不同领域表名需要修改
    cursor.execute(sql)
    result = cursor.fetchone()
    while result != None:
        paperKey.append(int(result[0]))
        if result[2] != None:
            docs.append(result[1] + '. ' + result[1] + '. ' + result[1] + '. ' + result[2])
        else:
            docs.append(result[1] + '. ' + result[1] + '. ' + result[1] + '.')
        result = cursor.fetchone()
    cursor.close()
    conn.close()

# 方法2，提取top-1000学者的论文
elif sys.argv[1] == '2':
    for i in range(1, 1001):
        df = pd.read_csv("./vis_input/papers_" + str(i) + ".csv", sep=',', index_col=0)
        df = df.fillna('')
        data = df.values.tolist()
        for row in data:
            paperKey.append(int(row[0]))
            if row[8] != None:
                docs.append(row[1] + '. ' + row[1] + '. ' + row[1] + '. ' + row[8])
            else:
                docs.append(row[1] + '. ' + row[1] + '. ' + row[1] + '.')

print(len(docs))
print(len(paperKey))

# TODO
# if os.path.exists("./model/acl_model"):
#     topic_model = BERTopic.load("./model/acl_model")
#     fig = topic_model.visualize_topics()
#     fig.write_html("./vis_output2/topics_visualization.html")
#     sys.exit()

# vectorizerModel = CountVectorizer(stop_words="english")
# topic_model = BERTopic(vectorizer_model=vectorizerModel, nr_topics=100)
# topics, probabilities = topic_model.fit_transform(docs)
# 预训练模型 all-mpnet-base-v2 paraphrase-MiniLM-L12-v2
topic_model = BERTopic(verbose=True, embedding_model="paraphrase-MiniLM-L12-v2", min_topic_size=100)
topics, probs = topic_model.fit_transform(docs)
topic_model.save("./model/visualization_model") #TODO 模型name

# 打印TopicID对应的Count和Name（去掉TopicID=-1，因为它都是一些to the and之类的topic）
df_topics = topic_model.get_topic_info()
df_topics = df_topics.loc[df_topics.Topic != -1, :]
df_topics.to_csv("./vis_output2/topic_count_name.csv", sep=',', index=False)

# 打印所有topic中每个topic中10个word-prob关系
topic_word_prob = []
for i in range(topic_model.get_topic_info().shape[0] - 1):
    topicID = topic_model.get_topic(i) #每个topic由10个word构成，打印第i个topic中每个(word, prob)
    dic = {}
    for word, prob in topicID:
        dic[word] = prob
    topic_word_prob.append(dic)
data = json.dumps(topic_word_prob, indent=4, separators=(',', ': '))
with open("./vis_output2/topic_word_prob.json", "w") as f:
    f.write(data)

# topic_distr是一个n x m矩阵，其中n是文档，m是主题
topic_distr, topic_token_distr = topic_model.approximate_distribution(docs, calculate_tokens=True)
max_idx = np.argmax(topic_distr, axis=1)    # 每个文档中哪个主题概率最大，一维向量
max_prob = np.amax(topic_distr, axis=1)     # 每个文档中概率最大的主题概率为多少，一维向量
print(topic_distr.shape, max_idx.shape, max_prob.shape)
# 将每个文档的最大topic作为该文档的topic
for i in range(1, 1001):
    df = pd.read_csv("./vis_input/papers_" + str(i) + ".csv", sep=',', index_col=0)
    # df.drop(columns=["fields"], axis=1, inplace=True)
    topicidx = []
    papers = df["paperID"].values.tolist()
    for paperID in papers:
        idx = paperKey.index(paperID)
        topicidx.append(max_idx[idx])
    df["topic"] = topicidx
    df.to_csv("./csv2/papers_" + str(i) + ".csv", sep=',')


# Embed c-TF-IDF into 2D
freq_df = topic_model.get_topic_freq()
freq_df = freq_df.loc[freq_df.Topic != -1, :]   # (n, 2)矩阵，两列为Topic和Count，按Count从大到小排序
freq_topics = sorted(freq_df.Topic.to_list())
all_topics = sorted(list(topic_model.get_topics().keys()))
indices = np.array([all_topics.index(topic) for topic in freq_topics])
embeddings = topic_model.c_tf_idf_.toarray()[indices]   # topic_model.c_tf_idf_为(n, m)矩阵，m很大，所以需要用下面的UMAP降维
embeddings = MinMaxScaler().fit_transform(embeddings)
embeddings = UMAP(n_neighbors=2, n_components=2, metric='hellinger').fit_transform(embeddings)

df_embeddings = pd.DataFrame(embeddings, columns=['x', 'y'])
merge_df = pd.concat([df_topics, df_embeddings], axis=1)
merge_df.to_csv("./vis_output2/topic_location.csv", sep=',', index=False)    # TODO location最后一行有问题，需要手动改


# 可视化topic分布2d图
fig = topic_model.visualize_topics()
fig.write_html("./vis_output2/topics_visualization.html")

# 层次树：所有生成的topic都是叶子节点，bertopic会自己给你总结上面的topic
hierarchical_topics = topic_model.hierarchical_topics(docs)
tree = topic_model.get_topic_tree(hierarchical_topics)
print(tree)
