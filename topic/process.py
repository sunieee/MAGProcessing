import pandas as pd
import json

df_topic_count_name = pd.read_csv("./vis_output2/topic_count_name.csv", sep=',')
topic_count_name = df_topic_count_name.values.tolist()
with open("./vis_output2/topic_word_prob_merged.json",'r+') as f:
    topic_word_proba = json.load(f)
data = []
for count, proba in zip(topic_count_name, topic_word_proba):
    topic = [count[0], count[1]]
    kv = [f'{key}({value:.4f})' for key, value in proba.items()]
    topic.append(', '.join(kv))
    data.append(topic)
df = pd.DataFrame(data, columns=["Topic", "Count", "Word(Proba)"])
df.to_csv("./vis_output2/topic-word.csv", sep=',', index=False)