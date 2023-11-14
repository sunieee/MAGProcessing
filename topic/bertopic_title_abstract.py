# 该文件可以提炼出所有给出的文本的主题的概括词及其二维分布，以及每个文本对应的topic
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
from sentence_transformers import SentenceTransformer

if len(sys.argv) != 3:
    print("Format: python bertopic_title_abstract field 1")
    sys.exit()

field = sys.argv[1]
directory = "input/" + field + "/"
out_directory = "output/" + field
paper_directory = "paper/" + field
model_path = "model"
for path in (out_directory, paper_directory):
    if os.path.exists(path) == False:
        os.makedirs(path)

paperKey = []
docs = []
# 方法1，提取所有该领域论文
# TODO 不同领域数据库需要修改
if sys.argv[2] == '1':
    conn = mysql.connector.connect(host="192.168.0.140",
                                   user="root",
                                   password="root",
                                   database="scigene_" + field + "_field")
    cursor = conn.cursor()
    sql = "select paperID, title, abstract from papers_field;"
    cursor.execute(sql)
    result = cursor.fetchone()
    while result != None:
        paperKey.append(int(result[0]))
        title = (result[1] + '. ') * 3
        if result[2] != None:
            docs.append(title + result[2])
        else:
            docs.append(title[:-1])
        result = cursor.fetchone()
    cursor.close()
    conn.close()
# 方法2，提取top学者的论文
elif sys.argv[2] == '2':
    files = os.listdir(directory)
    for file in files:
        df = pd.read_csv(os.path.join(directory, file), sep=',', index_col=0)
        df = df.fillna('')
        data = df.values.tolist()
        for row in data:
            paperKey.append(row[0])
            title = (row[1] + '. ') * 3
            if row[8] != None:
                docs.append(title + row[8])
            else:
                docs.append(title[:-1])
print(len(docs))
print(len(paperKey))

if os.path.exists(model_path):
    print("model exists!")
    topic_model = BERTopic.load(os.path.join(model_path, f'{field}_model'))
else:
# 预训练模型 all-mpnet-base-v2 paraphrase-MiniLM-L12-v2
# sentence_model = SentenceTransformer("model/all-mpnet-base-v2")
# embeddings = sentence_model.encode(docs, show_progress_bar=True)
# topic_model = BERTopic(verbose=True, min_topic_size=120)
# topics, probs = topic_model.fit_transform(docs, embeddings)
    print("model doesn't exist!")
    if os.path.exists("model") == False:
        os.mkdir("model")
    topic_model = BERTopic(verbose=True, embedding_model="paraphrase-MiniLM-L12-v2", min_topic_size=300, calculate_probabilities=True)
    topics, probs = topic_model.fit_transform(docs)
    topic_model.save(os.path.join(model_path, f'{field}_model'))

# topic_distr是一个n x m矩阵，其中n是文档，m是主题
topic_distr, topic_token_distr = topic_model.approximate_distribution(docs, calculate_tokens=True)
max_idx = np.argmax(topic_distr, axis=1)    # 每个文档中哪个主题概率最大，一维向量
max_prob = np.amax(topic_distr, axis=1)     # 每个文档中概率最大的主题概率为多少，一维向量
print(topic_distr.shape, max_idx.shape, max_prob.shape)
doc_info = topic_model.get_document_info(docs)
doc_topic_info = doc_info["Topic"].values
doc_info.drop(columns=['Document', 'Name', 'Top_n_words', 'Probability', 'Representative_document'], inplace=True)
doc_info['max_idx'] = max_idx
doc_info.to_csv(os.path.join(out_directory, "paper_topic.csv"), sep=',', index=False)
print(doc_info.shape, doc_topic_info.shape, doc_topic_info[0])
# 将每个文档的最大topic作为该文档的topic
files = os.listdir(directory)
for file in files:
    df = pd.read_csv(os.path.join(directory, file), sep=',')
    topicidx = []
    papers = df["paperID"].values.tolist()
    for paperID in papers:
        idx = paperKey.index(paperID)
        if doc_topic_info[idx] != -1 and doc_topic_info[idx] != 0:
            topicidx.append(doc_topic_info[idx])
        else:
            topicidx.append(max_idx[idx])
    df["topic"] = topicidx
    df.to_csv(os.path.join(paper_directory, file), sep=',')

# 打印TopicID对应的Count和Name（去掉TopicID=-1，因为它都是一些to the and之类的topic）
df_topics = topic_model.get_topic_info()
df_topics = df_topics.loc[df_topics.Topic != -1, :]
df_topics.to_csv(os.path.join(out_directory, "topic_count_name.csv"), sep=',', index=False)

# 打印所有topic中每个topic中10个word-prob关系
data = []
for i in range(topic_model.get_topic_info().shape[0] - 1):
    topicID = topic_model.get_topic(i)      # 每个topic由10个word构成，打印第i个topic中每个(word, prob)
    topic_word_prob = {word: prob for word, prob in topicID}
    data.append(topic_word_prob)
data = json.dumps(data, indent=4, separators=(',', ': '))
with open(os.path.join(out_directory, "topic_word_prob_raw.json"), "w") as f:
    f.write(data)

# 可视化topic分布2d图
fig = topic_model.visualize_topics(output_path=(sys.path[0] + "/output/" + field))
fig.write_html(os.path.join(out_directory, "topic_distribution.html"))

# 1.层次树：所有生成的topic都是叶子节点，bertopic会自己给你总结上面的topic
# hierarchical_topics = topic_model.hierarchical_topics(docs)
# tree = topic_model.get_topic_tree(hierarchical_topics)
# print(tree)

# 2.Embed c-TF-IDF into 2D
# freq_df = topic_model.get_topic_freq()
# freq_df = freq_df.loc[freq_df.Topic != -1, :]   # (n, 2)矩阵，两列为Topic和Count，按Count从大到小排序
# freq_topics = sorted(freq_df.Topic.to_list())
# all_topics = sorted(list(topic_model.get_topics().keys()))
# indices = np.array([all_topics.index(topic) for topic in freq_topics])
# embeddings = topic_model.c_tf_idf_.toarray()[indices]   # topic_model.c_tf_idf_为(n, m)矩阵，m很大，所以需要用下面的UMAP降维
# embeddings = MinMaxScaler().fit_transform(embeddings)
# embeddings = UMAP(n_neighbors=2, n_components=2, metric='hellinger').fit_transform(embeddings)

# df_embeddings = pd.DataFrame(embeddings, columns=['x', 'y'])
# merge_df = pd.concat([df_topics, df_embeddings], axis=1)
# merge_df.to_csv("./output/vis-mpnet-120/topic-location.csv", sep=',', index=False)    # TODO location最后一行有问题，需要手动改