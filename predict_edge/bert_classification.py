import transformers
import torch
from sklearn.ensemble import RandomForestClassifier
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import cross_val_score
import sklearn
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.svm import SVC
from sklearn.neural_network import MLPClassifier
import sklearn
from imblearn import under_sampling
from imblearn import over_sampling
from imblearn import pipeline
import random
from sklearn.metrics import *
import numpy
import matplotlib.pyplot as plt
import pickle
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.ensemble import GradientBoostingClassifier
import lightgbm
# lightgbm.LGBMClassifier
# from transformers import *
#
# tokenizer = AutoTokenizer.from_pretrained('allenai/scibert_scivocab_uncased')
# model = AutoModel.from_pretrained('allenai/scibert_scivocab_uncased')
# #
# df = pd.read_csv('../sentences_and_labels.csv',delimiter='\t',header = None)
# sklearn.utils.shuffle(df)
# batch_1 = df[:]
# print(batch_1[0].value_counts())
# model_class, tokenizer_class, pretrained_weights = (transformers.DistilBertModel, transformers.DistilBertTokenizer, 'distilbert-base-uncased')
# tokenizer = tokenizer_class.from_pretrained(pretrained_weights)
# model = model_class.from_pretrained(pretrained_weights)
# tokenized = batch_1[1].apply((lambda x: tokenizer.encode(x, add_special_tokens=True)))
# max_len = 0
# for i in tokenized.values:
#     if len(i) > max_len:
#         max_len = len(i)
# padded = np.array([i + [0]*(max_len-len(i)) for i in tokenized.values])
# print(np.array(padded).shape)
#
# attention_mask = np.where(padded != 0, 1, 0)
# print(attention_mask.shape)
#
# input_ids = torch.tensor(padded)
# attention_mask = torch.tensor(attention_mask)
#
# with torch.no_grad():
#     last_hidden_states = model(input_ids, attention_mask=attention_mask)
#
# features = last_hidden_states[0][:,0,:].numpy().tolist()

ALL_PROB = []
ALL_LABEL = []
ALL_TRUE_LABEL = []
ALL_PREDICTED = []
# labels = batch_1[0].tolist()

labels = []
features = []
# file = open('../bert_features_1.csv','w')
file = open('../bert_features_1.csv','r')
line = file.readline()
while len(line) > 0:
    line = line.strip('\n').split(',')
    labels.append(line[0])
    features.append(line[1:])
    line = file.readline()
# for i in range(len(features)):
#     file.writelines(','.join([labels[i]]+[str(features[i][x]) for x in range(len(features[0]))])+'\n')
file.close()
# exit(0)
for i in range(10):
    train_features, test_features, train_labels, test_labels = train_test_split(features, labels,test_size=0.1)

    clf = MLPClassifier(solver='adam',max_iter=1000,hidden_layer_sizes=(50))

    clf.fit(train_features,train_labels)
    plt.figure()
    plt.plot(clf.loss_curve_)
    plt.show()
    prob_predict = clf.predict_proba(test_features)
    test_predict = clf.predict(test_features)
    ALL_PREDICTED.extend(test_predict)
    ALL_TRUE_LABEL.extend(test_labels)
    print(sklearn.metrics.confusion_matrix(test_labels, test_predict))
    if test_predict[0] == 'Extends':
        if prob_predict[0][0] < prob_predict[0][1]:
            prob_predict = prob_predict[:,1]
        else:
            prob_predict = prob_predict[:,0]
    else:
        if prob_predict[0][0] < prob_predict[0][1]:
            prob_predict = prob_predict[:,0]
        else:
            prob_predict = prob_predict[:,1]
    name = {'Extends':1,'Others':0}
    prob_true_label = [name[x] for x in test_labels]
    ALL_PROB.extend(prob_predict)
    ALL_LABEL.extend(prob_true_label)
    recal_precision = sklearn.metrics.precision_recall_curve(prob_true_label, prob_predict)
    max_F = 0
    for i, j in zip(recal_precision[0], recal_precision[1]):
        try:
            if (2 * i * j) / (i + j) > max_F:
                max_F = (2 * i * j) / (i + j)
        except:
            continue
    print(max_F)
recal_precision = sklearn.metrics.precision_recall_curve(ALL_LABEL,ALL_PROB)
plt.plot(recal_precision[0],recal_precision[1])
max_F = 0
max_i = 0
max_j = 0
for i,j in zip(recal_precision[0],recal_precision[1]):
    if (2*i*j)/(i+j) > max_F:
        max_F = (2*i*j)/(i+j)
        max_i = i
        max_j = j
print([max_F,max_i,max_j])
plt.show()
plt.figure()
name = {'Extends':1,'Others':0}
prob_true_label = [name[x] for x in test_labels]
recal_precision = sklearn.metrics.precision_recall_curve(ALL_LABEL,ALL_PROB)
print(sklearn.metrics.roc_auc_score(ALL_LABEL,ALL_PROB))
plt.plot(recal_precision[0],recal_precision[1])
plt.show()
print(sklearn.metrics.confusion_matrix(ALL_TRUE_LABEL,ALL_PREDICTED))
all_data = []

for i,j in zip(labels,features):
    all_data.append([i]+j)
# del features
# del labels

sklearn.utils.shuffle(all_data)
exit(0)
test_percentage = 0.1

all_x_label = []
all_x_prediction = []
ALL_Y_REAL = []
ALL_Y_PREDICTED = []
ALL_Y_TRUE_PROBA = []
ALL_Y_PREDICTED_PROBA = []
count = 0
accscores = []
data = []
while count < 1/test_percentage:
    train_x = []
    train_y = []
    test_x = []
    test_y = []
    for index in range(len(features)):
        if index/len(features) >= (count+1)*test_percentage or index/len(features) < count * test_percentage:
            train_x.append(all_data[index][1:])
            train_y.append(all_data[index][0])
        else:
            test_x.append(all_data[index][1:])
            test_y.append(all_data[index][0])
# clf = RandomForestClassifier(n_estimators=500,class_weight={'Extends':2,'Others':1})
#     clf = SVC(class_weight={'Extends':3,'Others':1})
    clf = MLPClassifier(max_iter=1000,hidden_layer_sizes=(40))
    clf.fit(train_x,train_y)
    predicted_proba_y = clf.predict_proba(test_x)[:, 0]
    print('aaa')
    name = dict()
    name['Others'] = 0
    name['Extends'] = 1
    true_proba_y = [name[x] for x in test_y]
    ALL_Y_TRUE_PROBA.extend(true_proba_y)
    ALL_Y_PREDICTED_PROBA.extend(predicted_proba_y)
    predicted_y = clf.predict(test_x)
    all_x_label.extend(test_y)
    all_x_prediction.extend(predicted_y)
    print(sklearn.metrics.confusion_matrix(all_x_label, all_x_prediction, labels=['Others', 'Extends']))

    predicted_y = clf.predict(train_x)
    print(sklearn.metrics.confusion_matrix(train_y, predicted_y, labels=['Others', 'Extends']))

    count += 1

    predicted_y = clf.predict(test_x)
    accscores.append(accuracy_score(test_y, predicted_y))

    ALL_Y_REAL.extend(test_y)
    ALL_Y_PREDICTED.extend(predicted_y)

    labels = set(ALL_Y_REAL)
    labels |= set(ALL_Y_PREDICTED)
    labels = list(labels)

    macro_f1 = f1_score(ALL_Y_REAL, ALL_Y_PREDICTED, labels=labels, average='macro')
    micro_f1 = f1_score(ALL_Y_REAL, ALL_Y_PREDICTED, labels=labels, average='micro')
    macro_p = precision_score(ALL_Y_REAL, ALL_Y_PREDICTED, labels=labels, average='macro')
    micro_p = precision_score(ALL_Y_REAL, ALL_Y_PREDICTED, labels=labels, average='micro')
    macro_r = recall_score(ALL_Y_REAL, ALL_Y_PREDICTED, labels=labels, average='macro')
    micro_r = recall_score(ALL_Y_REAL, ALL_Y_PREDICTED, labels=labels, average='micro')

    print('Running accuracy after %d fold, %f macro F1 (P: %f, R: %f), %f micro F1 (P: %f, R: %f)' % (
        count, macro_f1, macro_p, macro_r, micro_f1, micro_p, micro_r))

    print(labels)
    print(sklearn.metrics.confusion_matrix(ALL_Y_REAL, ALL_Y_PREDICTED, labels=labels))
    print('---------------------\n\n')

# pe_debug.close()
# dimension_queue = sorted(range(len(data)), key=lambda k: data[k], reverse=True)
# feature_name_sorted = []
# for i in range(358):
#     feature_name_sorted.append([])
# feature_value_sorted = numpy.zeros(358)
# index = 0
# for sort_value in dimension_queue:
#     feature_name_sorted[index] = feature_list[sort_value]
#     feature_value_sorted[index] = data[sort_value]
#     index += 1
# plt.bar(range(20), feature_value_sorted[0:20], align='center')
# plt.title('Feature Importance')
# plt.xticks(range(20), feature_name_sorted[0:20], rotation=90)
# plt.tight_layout()
# plt.show()
print()
print("---")
print(clf)
print("ACCURACY", numpy.mean(accscores))
# print(clf.feature_importances_)
print("Macro F1: score", f1_score(ALL_Y_REAL, ALL_Y_PREDICTED, labels=labels, average='macro'))

print(' '.join(labels))
pp = sklearn.metrics.confusion_matrix(ALL_Y_REAL, ALL_Y_PREDICTED, labels=labels)

if labels[1] == 'Extends':
    precision = pp[1][1] / (pp[0][1] + pp[1][1])
    recall = pp[1][1] / (pp[1][0] + pp[1][1])
else:
    precision = pp[0][0] / (pp[1][0] + pp[0][0])
    recall = pp[0][0] / (pp[0][1] + pp[0][0])

print((2 * precision * recall) / (precision + recall))
# print(' '.join(['Positional', 'Essential']))
print(sklearn.metrics.confusion_matrix(ALL_Y_REAL, ALL_Y_PREDICTED, labels=labels))
pp = sklearn.metrics.precision_recall_curve(ALL_Y_TRUE_PROBA, ALL_Y_PREDICTED_PROBA)
max_F_measure = 0
max_precision = 0
max_recall = 0
for i in range(len(pp[0])):
    precision = pp[0][i]
    recall = pp[1][i]
    F_measure = (2 * precision * recall) / (precision + recall)
    if F_measure > max_F_measure:
        max_F_measure = F_measure
        max_yuzhi = i
        max_precision = precision
        max_recall = recall
print([max_F_measure, pp[2][max_yuzhi], max_precision, max_recall])
plt.plot(list(pp[0]), list(pp[1]))
plt.show()