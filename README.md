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


## 合并作者

* Check top VIS scholars: some have name duplication problem, need to merge in future

把top 1000 scholars间的相似名称作者找到，弄一个重复列表，然后合并作者 (只VIS@MAG, ACL数据集无此问题)

通过**最短编辑距离除以总长**得到lev_dis并过滤(<0.3)，按照煜文的**标题比对算法**计算了similarity，按照similarity降序排序得到这些比较相似的名字，见表格`merge_author/match_groups.csv`共6组，M.E. Groller应该也是，但没必要合并了(应该论文很少)，预估相同作者的阈值是：simlarity > 0.96 且 lev_dis < 0.1

|      | rank1  | rank2  | name1               | name2               | lev_dis     | similarity  |
| ---- | ---- | ---- | ------------------- | ------------------- | ----------- | ----------- |
| 0    | 6    | 138  | Daniel A. Keim      | Daniel A. Keim      | 0           | 1           |
| 1    | 31   | 401  | Sheelagh Carpendale | Sheelagh Carpendale | 0           | 1           |
| 2    | 149  | 944  | Takashi Yamane      | Takashi Yamane      | 0           | 1           |
| 3    | 610  | 761  | Shinzaburo Umeda    | Shinzaburo Umeda    | 0           | 1           |
| 8    | 148  | 234  | Eduard GrÃ¶ller     | M. Eduard GrÃ¶ller  | 0.090909091 | 1           |
| 58   | 78   | 143  | Jarke J. van Wijk   | J.J. van Wijk       | 0.166666667 | 0.909090909 |

![47d5e9e30a8bfc31b5929caeb72349a](https://github.com/sunieee/ARC/assets/42105752/218f75fd-2220-4e52-b7a3-3e184666e750)


如果不修改任何有关数据库，直接后处理合并生成的多张表格是不行的。因为在计算点概率的过程中用到了author表与coauthor的联合查询，合并之后影响点概率的计算，因此必须在第二步之前就必须将两人合为一人！

将少的合并到大的之中，步骤为：
- 修改领域数据库：在paper_author_field中修改author_id，重新刷新一下统计信息：#paper, #citation, hIndex
- 重新生成点概率：


## 增量更新

通过指定用户名、用户排名进行子图抽取，边概率不需要重新运算

pcg图需要合并：（点概率+边概率），只在这些人上run