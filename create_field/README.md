
# create field database


按照`run.sh`所示，构建过程分为以下5个步骤:
1. extract_scigene_field
  - 创建field基础数据库
  - 根据`config.yaml`搜索该field对应fieldID, children, ConferenceID, JournalID下的所有论文paperID，保存在`papers.txt`
  - 从mysql中获取papers, paer_auther, paper_reference, authors四个表的field子数据，并保存到本地文件（使用get_data_from_table_concurrent将大查询分解为每次查询2000个paperID，使用20个线程并行）
  - 读取四个子表，并上传到mysql。创建表后添加领域子表的mysql索引
2. extract_citation_timeseries
  - 从论文引用和发布日期信息中创建了一个名为papers_field_citation_timeseries_raw的数据表，记录了每篇论文每年的引用次数
  - 从raw中提取、处理并创建名为 papers_field_citation_timeseries 的数据表，记录了每篇论文在不同年份的引用次数信息（当前引用、总引用、引用序列），同时保证年份的连续性和数据的完整性
3. renew_database
  - 将论文表中的年份更新为发布日期的年份，为年份添加索引，然后从papers_field_citation_timeseries表复制引用次数序列数据
  - 计算并添加作者在领域内的论文及引用数量，更新作者的引用总数信息
  - 通过计算每位作者的引用次数数据，根据 h-index 的定义，计算并更新了每位作者在特定领域内的 h-index 值
4. match_author/merge_author：
  - in MAG, some people have name duplication problem, need to merge
  - 匹配hIndex前3000人是否有重复的人，如果重复则合并（详细过程见后）


上述表格式参考已建立的下述表格式

```
+---------------------------------+
| Tables_in_scigene_acl_anthology |
+---------------------------------+
| authors_field                     |
| paper_author_field                |
| paper_reference_field             |
| papers_field                      |
| papers_field_citation_timeseries  |
+---------------------------------+
```


> 由于mysql语句中间有多个数据库都是中间结果，创建之后又删掉。在数据库中频繁地创建临时表、写入数据、删除表可能会导致磁盘的反复写入，增加了系统资源的开销，同时也会影响代码执行的效率。使用 python 的 pandas 导入数据并处理成 DataFrame 的方式可能会更加高效和灵活，尤其是在执行数据清洗、转换和分析等操作时。

## match_author/merge_author

```sh
pip install gensim
pip install Levenshtein
```

把top 3000 scholars间的相似名称作者找到，弄一个重复列表，然后合并作者

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

运行`match_author.py`之后生成两个表格：
- `out/{database}/groups.csv` 是所有pair
- `out/{database}/match.csv` 是按照条件（simlarity > 0.96 且 lev_dis < 0.1）筛选后的pair

需要进一步**手动**筛选，选出最终需要合并的人，放到`out/{database}/filtered.csv`，筛选规则：
1. 对于 完全一致(similarity = 1 & lev_dis = 0)的人名，删除所有中文人名，基本上都是重名
2. 其他人名手动筛选，看看是不是缩写，或者多了一个名，在 google scholar 上搜索一下，看看是不是同一个人

最后运行`merge_author.py`即可


## 进程与线程

1. 进程不能传递pbar（只能传递可序列化的变量），不能使用进度条，否则出错TypeError: cannot pickle '_io.TextIOWrapper' object
2. 多线程即使将max_worker设置为 20，也无法跑满全部mysql，利用率在100%左右。整体上弱于多进程
3. 必须在多进程和多线程函数内部创建mysql链接，否则会有进程锁
4. 不能使用可变全局变量，只能使用只读全局变量

```
pbar = tqdm(total=group_num)
params = [(data[i*GROUP_SIZE:(i+1)*GROUP_SIZE], i, pbar) for i in range(group_num)]
with concurrent.futures.ThreadPoolExecutor(max_workers=multiproces_num * 5) as executor:
    results = executor.map(_query, params)

with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(extract_paper_year, [(paperID_list[i*group_size:(i+1)*group_size], f'{i}/{group_length}') for i in range(group_length)])
```

