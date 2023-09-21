# ARC数据处理流程

## 名词解释

MAG

hIndex

ARC



## 构建图模型

注意：除了建领域数据库会存一份到数据库当中，其他几步直接以csv数据表的形式存到本地

1. 建领域数据库：`192.168.0.118:/home/xiaofengli/pyCode/create_field`

2. 抽子图：`192.168.0.118:/home/xiaofengli/pyCode/parser`
    - build-top-author：生成前多少名scholar
    - compute-top-key-paper：计算点概率（每个学者论文的isKeyPaper）
    - dump：下载成csv

3. 抽边特征：`192.168.0.118:/home/xiaofengli/pyCode/field_edge/all_features.sh`
    - （计算步骤，6个py）
    - 但也可以直接使用已经抽取好的特征/home/xiaofengli/pyCode/field_edge/all_features.csv

4. 计算边概率：`/home/xfl/download/acl/compute_proba.py`

边概率训练：...

```python
file = open('saved_model_another_5depth.pickle','rb')
model = pickle.load(file)
result=model.predict_proba(db2)[:,1]
```

边概率模型：https://github.com/tinyApril/GeneticFlow/tree/main/Award_Inference_Task/Award_Inference_Experiment/GeneticFlow_Signatures/GNN_NLP_data/data_with_proba

`saved_model.pickle`, `saved_model_5depth.pickle` 适用于一切MAG数据库


