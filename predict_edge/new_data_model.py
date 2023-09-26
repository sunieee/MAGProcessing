from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
import sklearn
from sklearn.linear_model import LogisticRegressionCV
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
import csv
from tqdm import tqdm
# lightgbm.LGBMClassifier
# create table new_combined_dataset(citingpaperID varchar(100), citedpaperID varchar(100),label varchar(50), num_Occur int,cross_correlation float,window_cross_correlation float,year_diff int,citing_paper_citationcount int,cited_paper_citationcount int,self_cite int,similarity float,jaccard_cocitation float,jaccard_bibcoupling float,average_section_citation float,average_subsection_citation float,average_sentence_citation float,min_relative_pos_section float,maxminusmin_relative_pos_section float,min_relative_pos_subsection float,maxminusmin_relative_pos_subsection float,min_relative_pos_sentence float,maxminusmin_relative_pos_sentence float,cite_times_in_Abstract int,cite_times_in_Introduction int,cite_times_in_Related_Work int,cite_times_in_Motivation int,cite_times_in_Method int,cite_times_in_Experiment int,cite_times_in_Evaluation int,cite_times_in_Conclusion int,cite_times_in_Acknowledgement int,key_phrase_1 int,key_phrase_2 int,key_phrase_3 int,key_phrase_4 int,key_phrase_5 int,key_phrase_6 int,key_phrase_7 int,key_phrase_8 int);

feature_select_list = [3,4,7,10,11,12,13,14,17,20]

# 下面这个是目前用到的所有feature的名字，可以对照着看一下理解一下意思

feature_list = ['num_Occur','cross_correlation','window_cross_correlation',
                'year_diff',
 'citing_citcount','cited_citcount','self_cite','similarity',
 'jaccard_cocitation','jaccard_bibcoupling',
 'average_section_citation','average_subsection_citation','average_sentence_citation',
 'min_relative_pos_section','maxminusmin_relative_pos_section',
 'min_relative_pos_subsection','maxminusmin_relative_pos_subsection',
 'min_relative_pos_sentence','maxminusmin_relative_pos_sentence',
 'cite_times_in_Abstract','cite_times_in_Introduction','cite_times_in_Related_Work' ,
 'cite_times_in_Motivation','cite_times_in_Method','cite_times_in_Experiment' ,
 'cite_times_in_Evaluation','cite_times_in_Conclusion','cite_times_in_Acknowledgement' ,
 'key_phrase_1','key_phrase_2','key_phrase_3','key_phrase_4','key_phrase_5' ,
 'key_phrase_6','key_phrase_7','key_phrase_8']
# out = feature_list.index('cross_correlation')
chosen = [i for i in range(len(feature_list))]
# chosen = [i for i in range(len(feature_list)-8,len(feature_list))]
feature_list = [feature_list[x] for x in chosen]
file = open('../all_dataset_link_label_with_combined_features.txt','r')
# file = open('../all_ARC_ARC.txt','r')
line = file.readline()
special = line.split('+')
average_list = [0 for _ in range(len(special)-4)]
count = [0 for _ in range(len(average_list))]
total_how_many_number = 0
min_list = [100 for _ in range(len(special)-4)]
while len(line) > 3:
    total_how_many_number += 1
    line = line.strip().split('+')
    for index in range(3,len(line)-1):
        if not line[index][0].isdigit():
            average_list[index-3] += 0
            continue
        if float(line[index]) < min_list[index-3]:
            min_list[index-3] = float(line[index])
        average_list[index-3] += float(line[index])
        count[index-3] += 1
    line = file.readline()
print(count)
for index in range(len(average_list)):
    average_list[index] = average_list[index] / count[index]
file.close()
file = open('../all_dataset_link_label_with_combined_features.txt','r')
line = file.readline()
line = file.readline()
# data_file = open('../final_dataset.txt','w')
#--------------------------
test_percentage = 0.1
#--------------------------
total_how_many_number = 0
all_data = []
all_label = []

## 数据抽取和缺失值替换
while len(line) > 3:
    total_how_many_number += 1
    line = line.strip().split('+')
    #if int(line[21]) > 0:
    #    line = file.readline()
    #    continue
    # if not int(line[21]) == 6 and line[2] == 'Extends':
    #     line = file.readline()
    #     continue
    for index in range(3,len(line)):
        # if not line[index][0].isdigit() or line[index] == -2:
        if line[index] == '-2' or line[index] == '\\N':
            line[index] = -2 + 2*random.random()
            # line[index] = -2
            # line[index] = '\\\\N'
            continue
        line[index] = float(line[index])
    # all_data.append([line[x] for x in [2]+feature_select_list])
    list_chosen = [3, 4, 7, 10, 11, 12, 13, 14, 17, 20]  + [i for i in range(22, 30)] + [i for i in range(30, len(line))]
    # list_chosen = [3, 4, 7, 10, 11, 12, 13, 14, 17, 20] + [i for i in range(22, 30)] + [i for i in range(30, len(line))]
    # list_chosen = [i for i in range(len(lixxxne)-8,len(line))]
    all_data.append([line[x] for x in [2]+list_chosen])
    # data_file.writelines('+'.join([str(line[x]) for x in [0,1,2] + list_chosen])+'\n')
    # all_data.append([line[x] for x in [2,3,4,7,10,11,12,13,14,17,20]+[i for i in range(22,len(line))]])
    line = file.readline()
# data_file.close()
all_data_others = []
all_data_extends = []
# all_data_file = open('../complete_all_data_file.csv','w')
# all_data_file.writelines(','.join(['label']+feature_list)+'\n')
for data in all_data:
    # all_data_file.writelines(','.join([str(x) for x in data]) + '\n')
    if data[0] == 'Others':
        all_data_others.append(data)
    else:
        all_data_extends.append(data)

# all_data_file.close()


failing_data = []
random.shuffle(all_data)
random.shuffle(all_data_extends)
random.shuffle(all_data_others)
all_x_label = []
all_x_prediction = []
ALL_Y_REAL = []
ALL_Y_PREDICTED = []
ALL_Y_TRUE_PROBA = []
ALL_Y_PREDICTED_PROBA = []
count = 0
accscores = []
data = []


## 10-fold-crossvalidation 分类过程
while count < 1/test_percentage:
    train_x = []
    train_y = []
    test_x = []
    test_y = []
    for index in tqdm(range(len(all_data_others))):
        if index/len(all_data_others) >= (count+1)*test_percentage or index/len(all_data_others) < count * test_percentage:
            if random.random() > 1:
                continue
            train_x.append(all_data_others[index][1:])
            train_y.append(all_data_others[index][0])
        else:
            test_x.append(all_data_others[index][1:])
            test_y.append(all_data_others[index][0])
    for index in range(len(all_data_extends)):
        if index/len(all_data_extends) >= (count+1)*test_percentage or index/len(all_data_extends) < count * test_percentage:
            train_x.append(all_data_extends[index][1:])
            train_y.append(all_data_extends[index][0])
        else:
            test_x.append(all_data_extends[index][1:])
            test_y.append(all_data_extends[index][0])
    clf = ExtraTreesClassifier(n_estimators=800, n_jobs=-1,max_depth=10,
                               class_weight={'Extends': 1.2, 'Others': 1})
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
    for index,label in enumerate(predicted_y):
        if label != test_y[index]:
            failing_data.append([test_y[index]]+test_x)
    all_x_label.extend(test_y)
    all_x_prediction.extend(predicted_y)
    print(sklearn.metrics.confusion_matrix(all_x_label, all_x_prediction,labels=['Others','Extends']))

    predicted_y = clf.predict(train_x)
    print(sklearn.metrics.confusion_matrix(train_y, predicted_y,labels=['Others','Extends']))

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
dimension_queue = sorted(range(len(data)), key=lambda k: data[k], reverse=True)
feature_name_sorted = []
for i in range(358):
    feature_name_sorted.append([])
feature_value_sorted = numpy.zeros(358)
index = 0
for sort_value in dimension_queue:
    feature_name_sorted[index] = feature_list[sort_value]
    feature_value_sorted[index] = data[sort_value]
    index += 1
plt.bar(range(20), feature_value_sorted[0:20], align='center')
plt.title('Feature Importance')
plt.xticks(range(20), feature_name_sorted[0:20], rotation=90)
plt.tight_layout()
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

fpr,tpr,thres = roc_curve(ALL_Y_TRUE_PROBA,ALL_Y_PREDICTED_PROBA)
roc_auc =  auc(fpr,tpr)
print(roc_auc)
# print(' '.join(['Positional', 'Essential']))
with open('../saved_model.pickle','wb') as f:
    pickle.dump(clf,f)
print(sklearn.metrics.roc_auc_score(ALL_Y_TRUE_PROBA,ALL_Y_PREDICTED_PROBA))
print(sklearn.metrics.confusion_matrix(ALL_Y_REAL, ALL_Y_PREDICTED, labels=labels))
pp = sklearn.metrics.precision_recall_curve(ALL_Y_TRUE_PROBA, ALL_Y_PREDICTED_PROBA)
precision_1,recall_1,_ = sklearn.metrics.precision_recall_curve(ALL_Y_TRUE_PROBA, ALL_Y_PREDICTED_PROBA)
# disp = PrecisionRecallDisplay(precision=precision_1, recall=recall_1)
# disp.plot()
# plt.show()
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
print([max_F_measure,pp[2][max_yuzhi],max_precision,max_recall])
print('%.4f %.4f' %(max_F_measure,sklearn.metrics.roc_auc_score(ALL_Y_TRUE_PROBA,ALL_Y_PREDICTED_PROBA)))
predicted_proba_new = [round(x) for x in ALL_Y_PREDICTED_PROBA]
print(precision_score(ALL_Y_TRUE_PROBA,predicted_proba_new))
precision,recall,f1,sup = precision_recall_fscore_support(ALL_Y_TRUE_PROBA,predicted_proba_new)
#with open(r'C:\Users\Desktop\citation_function\final_data_add/3_data.csv','a') as a:
#    a.writelines('%.4f,%.4f,%.4f,%.4f,%.4f,%.4f\n' %(max_F_measure,roc_auc_score(ALL_Y_TRUE_PROBA,ALL_Y_PREDICTED_PROBA),max_precision,max_recall,average_precision_score(ALL_Y_TRUE_PROBA,ALL_Y_PREDICTED_PROBA),accuracy_score(ALL_Y_REAL,ALL_Y_PREDICTED)))
print('max_F_measure: %.4f\nauc_score: %.4f\nprecision: %.4f\nrecall: %.4f\naverage precision: %.4f\naccuracy: %.4f\n' %(max_F_measure,roc_auc_score(ALL_Y_TRUE_PROBA,ALL_Y_PREDICTED_PROBA),max_precision,max_recall,average_precision_score(ALL_Y_TRUE_PROBA,ALL_Y_PREDICTED_PROBA),accuracy_score(ALL_Y_REAL,ALL_Y_PREDICTED)))
