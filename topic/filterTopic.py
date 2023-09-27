# 尽量选够5个word，超过5个word的topic需要过滤prob<0.01的word
import json

path = "./vis_output2/"
with open(path + "topic_word_prob_manual.json", 'r') as f:
    data = json.load(f)

res = []
for wordProbDict in data:
    newWordProbDict = {}
    for word, prob in wordProbDict.items():
        if (len(newWordProbDict) < 5) or (len(newWordProbDict) >= 5 and prob >= 0.01):
            newWordProbDict[word] = prob
    res.append(newWordProbDict)

data = json.dumps(res, indent=4)
with open(path + "topic-word-prob-filtered.json", "w") as f:
    f.write(data)