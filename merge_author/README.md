
```
pip install gensim
pip install Levenshtein
```

## 合并作者

* Check top VIS scholars: some have name duplication problem, need to merge in future

把top 1000 scholars间的相似名称作者找到，弄一个重复列表，然后合并作者 (只VIS@MAG, ACL数据集无此问题)

通过**最短编辑距离除以总长**得到lev_dis并过滤(<0.3)，按照煜文的**标题比对算法**计算了similarity，按照similarity降序排序得到这些比较相似的名字，见表格`merge_author/match_groups.csv`共6组，M.E. Groller应该也是，但没必要合并了(应该论文很少)，预估相同作者的阈值是：simlarity > 0.96 且 lev_dis < 0.1

结果见`match.csv`

|| rank1 | rank2 | name1 | name2               | lev_dis             | similarity  |
| ----- | ----- | ----- | ------------------- | ------------------- | ----------- | ----------- |
| 0     | 6     | 138   | Daniel A. Keim      | Daniel A. Keim      | 0           | 1           |
| 1     | 31    | 401   | Sheelagh Carpendale | Sheelagh Carpendale | 0           | 1           |
| 2     | 149   | 944   | Takashi Yamane      | Takashi Yamane      | 0           | 1           |
| 3     | 610   | 761   | Shinzaburo Umeda    | Shinzaburo Umeda    | 0           | 1           |
| 8     | 148   | 234   | Eduard GrÃ¶ller     | M. Eduard GrÃ¶ller  | 0.090909091 | 1           |
| 58    | 78    | 143   | Jarke J. van Wijk   | J.J. van Wijk       | 0.166666667 | 0.909090909 |

![47d5e9e30a8bfc31b5929caeb72349a](https://github.com/sunieee/ARC/assets/42105752/218f75fd-2220-4e52-b7a3-3e184666e750)


如果不修改任何有关数据库，直接后处理合并生成的多张表格是不行的。因为在计算点概率的过程中用到了author表与coauthor的联合查询，合并之后影响点概率的计算，因此必须在第二步之前就必须将两人合为一人！

```sh
# 融合作者：修改author_id（可删除authorID2对应的pcg表）
python merge_author.py

# 增量更新：将新数据库的paper_author同步过来
python migrate_MACG2field.py

# 重新计算authors_field字段，包括：#paper, #citation, hIndex
python update_authors_field.py
```

通过指定用户名、用户排名进行子图抽取，边概率不需要重新运算

pcg图需要合并：重新计算点概率，只在这些人上run


## 问题

出现一个paperID缺少第一作者

```
SELECT * FROM paper_author_field where paperID='2065995114';
```

| paperID    | authorID   | authorOrder |
| ---------- | ---------- | ----------- |
| 2065995114 | 2147343253 | 3           |
| 2065995114 | 2172281910 | 2           |

因此在个人表中，存在firstAuthorID, firstAuthorName为空（null）

```
select paperID, year, firstAuthorID from papers_danielakeim6
```

缺少关键信息firstAuthorID，没办法计算，因此跳过：

```python
if not paper_row[2]:
    print('Target paper do not have first author, skip!', paperID)
    continue
firstAuthorID = str(paper_row[2].strip())

if firstAuthorID == topAuthorID:
```