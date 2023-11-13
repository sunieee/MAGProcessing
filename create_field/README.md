
# create field database

The create_field process in the MAG Data Processing Pipeline involves establishing a specialized database tailored to specific academic fields. It involves extracting relevant paper IDs based on field, conference, and journal identifiers, and managing large data sets with efficient querying and parallel processing. This sets the foundation for structured data analysis within the specified academic field.

## pipeline

1. **extract_scigene_field**:
  - Creates a basic database for the field.
  - Searches for papers under the field's specific fieldID, children, ConferenceID, and JournalID as per `config.yaml`, and saves the paper IDs in `papers.txt`.
  - Retrieves subsets of the papers, paper_authors, paper_reference, and authors tables from MySQL and saves them to local files. It breaks down large queries into smaller ones, each querying 2000 paper IDs, using 20 concurrent threads with `get_data_from_table_concurrent`.
  - Reads the subsets of these tables and uploads them to MySQL, creating indices for the field-specific sub-tables after table creation.
2. **extract_citation_timeseries**:
  - Creates a table named `papers_field_citation_timeseries_raw`, which records annual citation counts for each paper, based on citation and publication date information.
  - Extracts and processes data from the raw table to create `papers_field_citation_timeseries`, recording the citation counts of each paper across different years (current citations, total citations, citation sequence) while ensuring continuity and completeness of the data for each year.
3. **renew_database**:
  - Updates the publication year in the papers table and adds indices for the years. Copies citation sequence data from the `papers_field_citation_timeseries` table.
  - Calculates and adds the number of papers and citations for each author within the field, updating the total citation counts for authors.
  - Computes and updates each author's h-index in the specific field based on their citation data, adhering to the definition of h-index.
4. **match_author/merge_author**:
  - Addresses the issue of name duplication in MAG by merging records.
  - Checks for and merges duplicates among the top 3000 authors based on their h-index (details of the process are provided later).

By executing the aforementioned steps, the database tables have been established, adhering to the naming conventions outlined below. Notice: The titles of these steps correspond directly to the names of the Python files used.

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

