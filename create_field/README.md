
# field数据集个人基因图构建

[toc]

![image](https://github.com/tinyApril/GeneticFlowVis/assets/42105752/24661bc5-b2d8-4d92-b5a2-24ecfa57624a)

如图所示，构建过程分为：prepare, extract field, extract timeseries, update hIndex四个步骤。

由于mysql语句中间有多个数据库都是中间结果，创建之后又删掉。在数据库中频繁地创建临时表、写入数据、删除表可能会导致磁盘的反复写入，增加了系统资源的开销，同时也会影响代码执行的效率，对性能和资源消耗产生一定的负面影响。

使用 python 的 pandas 导入数据并处理成 DataFrame 的方式可能会更加高效和灵活，尤其是在执行数据清洗、转换和分析等操作时。当您需要在多个步骤之间传递数据时，可以考虑使用 pandas 的 DataFrame，将中间结果保留在内存中，而不是频繁地在数据库中创建临时表。

在prepare之后，其余三个步骤可直接运行脚本完成（使用pandas优化了直接mysql命令的过程，减少对数据库的压力，提高代码的执行效率）：

```sh
sh create_field.sh <fieldID> <database>
# sh create_field.sh 77088390 scigene_database_field

# 等同于顺序执行3个python脚本
python extract_scigene_field.py $1 $2
python extract_citation_timeseries.py $1 $2
python update_top_author_field_hIndex.py $1 $2
```
在上述命令中，将`<fieldID>`替换为prepare步骤中查询到的field ID, 将`<database>`替换为新创建的database名称

## 1. prepare

### create database
创建field数据库，如：
create database scigene_database_field;

```sql
create database scigene_database_field
create database scigene_ComputerGraphicsImages_field;
create database scigene_ComputerSecurity_field;
create database scigene_DataMining_field;
create database scigene_HCI_field;
create database scigene_ProgrammingLanguage_field;
create database scigene_SoftwareEngineering_field;
create database scigene_SpeechRecognition_field;
create database scigene_TheoreticalComputerScience_field;
create database scigene_molecular_physics_zh_field;
```

### search filedID
根据领域名称查询fieldID，如：
select * from field_of_study where name='Database';

```sql
-- 先查找fieldID：
select * from field_of_study where level = 0;
-- 找到"Computer science"id是"41008148",继续找子学科：
select * from field_children as fc, field_of_study as fs where fc.parentID="41008148" and fc.childrenID=fs.fieldID;
-- 找到"Database"的id是77088390
select * from field_of_study where name='Database';

scigene_ComputerGraphicsImages_field 121684516
scigene_ComputerSecurity_field 38652104
scigene_DataMining_field 124101348
scigene_HCI_field 107457646
scigene_ProgrammingLanguage_field 199360897
scigene_SoftwareEngineering_field 115903868
scigene_SpeechRecognition_field 28490314
scigene_NLP_field 204321447
scigene_TheoreticalComputerScience_field 80444323，继续找paper：
scigene_molecular_physics_zh_field 41999313，继续找paper：
```

## 2. extract field

创建MAG数据库中领域（如database）的镜像

可以直接在mysql中创建子数据表，如：
```sql
select * from papers_field as pf, papers as p where pf.fieldID = '77088390' and pf.paperID=p.paperID
```

但是该方法速度慢，改进方法见python(python里面直接在mysql创建四个表并加索引) extract_scigene_field.py 

### extract paperID

通过fieldID获取到该子领域的所有paperID，如
select paperID from paper_field where fieldID = '77088390'


### get_data_from_table

从mysql中获取papers, paer_auther, paper_reference, authors四个表的field子数据，并保存到本地文件

```python
for i in tqdm(range(0, len(data), GROUP_SIZE)):
    # get a group of MAG paperID
    MAG_group = data[i:i+GROUP_SIZE]

    sql=f'''select * from {table_name} where '''\
          + key + ' in ('+','.join([f'\'{x}\'' for x in MAG_group])+')'

    db_temp = pd.read_sql_query(sql, engine_MAG)
    db=pd.concat([db,db_temp])
```

在python文件中使用了get_data_from_table函数，将大查询分解为每次查询100个paperID，能够加快速度。并且进一步使用get_data_from_table_concurrent将最多20个查询并行起来（但不能确定并行查询能够节约时间）

### to_sql
读取四个子表，并上传到mysql。创建表后添加领域子表的mysql索引（例如在scigene_database_field库）
```sql
ALTER TABLE papers_field ADD CONSTRAINT papers_field_pk PRIMARY KEY (paperID);
alter table papers_field add index(citationCount);
alter table paper_author_field add index(paperID);
alter table paper_author_field add index(authorID);
alter table paper_author_field add index(authorOrder);
alter table authors_field add index(authorID);
alter table authors_field add index(name);
alter table paper_reference_field add index(citingpaperID);
alter table paper_reference_field add index(citedpaperID);
ALTER TABLE paper_reference_field ADD CONSTRAINT paper_reference_field_pk PRIMARY KEY (citingpaperID,citedpaperID);
```

更新name_short:（ARC遗留问题，现在可跳过）
```sh
python update_short_author_name_MAG.py
```

再增加papers_field表的abstract列并更新abstract：（目前140上没有MACG.abstract表，也可以跳过）
```sql
alter table papers_field ADD abstract mediumtext;
update papers_field as P, MACG.abstracts as abs set P.abstract = abs.abstract where P.paperID = abs.paperID;
```

## 3. extract timeseries

创建按年份citation表（papers_field_citation_timeseries）

### create timeseries raw

从论文引用和发布日期信息中创建了一个名为papers_field_citation_timeseries_raw的数据表，记录了每篇论文每年的引用次数，同时去除了无效的年份数据。

```sql
create table papers_field_citation_timeseries_raw_raw select M.paperID, R.citingpaperID, 0 as year from papers_field as M join paper_reference_field as R on M.paperID = R.citedpaperID;
create index id_index on papers_field_citation_timeseries_raw_raw(paperID);
create index citingid_index on papers_field_citation_timeseries_raw_raw(citingpaperID);
create index year_index on papers_field_citation_timeseries_raw_raw(year);

create table MACG_papers_tmpid select citingpaperID as paperID from papers_field_citation_timeseries_raw_raw group by citingpaperID;
create index id_index on MACG_papers_tmpid(paperID);
create table MACG_papers_tmp select P.* from MACG.papers as P join MACG_papers_tmpid as D on P.paperID = D.paperID;
create index id_index on MACG_papers_tmp(paperID);

update papers_field_citation_timeseries_raw_raw as C, MACG_papers_tmp as P set C.year = year(P.PublicationDate) where C.citingpaperID = P.paperID;
create table papers_field_citation_timeseries_raw select paperID, year, count(*) as cited_cnt from papers_field_citation_timeseries_raw_raw group by paperID, year;
delete from papers_field_citation_timeseries_raw where year <= 0;
```

### create timeseries

从raw中提取、处理并创建名为 papers_field_citation_timeseries 的数据表，记录了每篇论文在不同年份的引用次数信息（当前引用、总引用、引用序列），同时保证年份的连续性和数据的完整性。

```python
for paperID in tqdm(citationCountMap):

    publicationYear = paperYearMap[paperID]
    citationYearMap = citationCountMap[paperID]
    yearList = sorted(citationYearMap.keys())

    ...

    citeStartYear = yearList[0]
    citeEndYear = yearList[len(yearList) - 1]

    totalCitationCount = citationYearMap[yearList[0]]
    citationCountByYear = str(citationYearMap[yearList[0]])

    last_year = yearList[0]
    current_year = -1

    for index in range(1, len(yearList)):

        current_year = yearList[index]
        totalCitationCount += citationYearMap[current_year]

        if current_year > (last_year + 1):
            citationCountByYear += ",0" * (current_year - last_year - 1)

        citationCountByYear += "," + str(citationYearMap[current_year])

        last_year = current_year

    values = (paperID, publicationYear, citeStartYear, citeEndYear, totalCitationCount, citationCountByYear)
    cursor.execute(
        "insert into papers_field_citation_timeseries values(%s, %s, %s, %s, %s, %s)",
        values
    )
```


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

### update papers_field
将论文表中的年份更新为发布日期的年份，为年份添加索引，然后从papers_field_citation_timeseries表复制引用次数序列数据到新的列中。

加year：
```sql
ALTER TABLE papers_field ADD year INT;
update papers_field set year = year(PublicationDate);
alter table papers_field add index(year);
```
加citationCountByYear ：
```sql
ALTER TABLE papers_field ADD citationCountByYear varchar(999);
update papers_field as PA, papers_field_citation_timeseries as PM set PA.citationCountByYear = PM.citationCountByYear where PA.paperID = PM.paperID;
```

## 4. update hIndex
更新领域前1000作者hIndex（authors_field表信息）

### update authors_field
计算并添加作者在领域内的论文数量及排名，更新作者的引用总数信息，添加 h-index 信息，创建名为 paper_reference_field_labeled 的表，添加名为 FellowType 列

更新authors_field表PaperCount_field, authorRank：
```sql
create table authors_field_tmp select tmp.*, @curRank := @curRank + 1 AS authorRank from (select authorID, count(*) as PaperCount_field from paper_author_field group by authorID order by PaperCount_field desc) as tmp, (SELECT @curRank := 0) r;
create index author_index on authors_field_tmp(authorID);
ALTER TABLE authors_field ADD PaperCount_field INT;
ALTER TABLE authors_field ADD authorRank INT;
update authors_field, authors_field_tmp set authors_field.PaperCount_field = authors_field_tmp.PaperCount_field, authors_field.authorRank = authors_field_tmp.authorRank where authors_field.authorID = authors_field_tmp.authorID;
alter table authors_field add index(authorRank);
drop table authors_field_tmp;
```
更新authors_field表CitationCount_field：
```sql
create table authors_field_tmp select sum(P.citationCount) as CitationCount_field,  authorID from papers_field as P join paper_author_field as PA on P.paperID = PA.paperID and P.CitationCount >=0 group by authorID;
create index id_index on authors_field_tmp(authorID);
ALTER TABLE authors_field ADD CitationCount_field INT;
update authors_field, authors_field_tmp set authors_field.CitationCount_field = authors_field_tmp.CitationCount_field where authors_field.authorID = authors_field_tmp.authorID;
drop table authors_field_tmp;
```
### calculate hIndex

通过计算每位作者的引用次数数据，根据 h-index 的定义，计算并更新了每位作者在特定领域内的 h-index 值，以反映其影响力和论文引用分布情况。

```sql
ALTER TABLE authors_field ADD hIndex_field INT;
python update_top_author_field_hIndex.py 1000

生成paper_reference_field_labeled：
create table paper_reference_field_labeled(
citingpaperID varchar(15),
citedpaperID varchar(15),
extends_prob double
);
```

设置FellowType（需要fellow并且在领域引用大于3000，需要用groupby检查唯一性）：
```sql
-- (检查：
select * from authors_field as af, scigene_acl_anthology.fellow as f where af.name = f.name and af.authorRank<=200 and f.type=1 and af.CitationCount_field>=3000;
select af.name from authors_field as af, scigene_acl_anthology.fellow as f where af.name = f.name and af.authorRank<=200 and f.type=1;
select af.name from authors_field as af, scigene_acl_anthology.fellow as f where af.name = f.name and af.authorRank<=200 and f.type=1 group by (af.name);
-- ）

ALTER TABLE authors_field ADD FellowType varchar(999);
update authors_field as af, scigene_acl_anthology.fellow as f set af.FellowType='1' where af.name = f.name and af.authorRank<=200 and f.type=1 and CitationCount_field>=3000;
```

## 5. merge author

```sh
pip install gensim
pip install Levenshtein

./merge.sh
```

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

1. 通过指定用户名、用户排名进行子图抽取，边概率不需要重新运算
2. pcg图需要合并：重新计算点概率，只在这些人上run




# 构建样例

创建scigene_database_field、scigene_physics_field两个数据库，分别对于的fieldID为：77088390、121332964，定义在`config.yaml`

以database为例：

```sql
create database scigene_database_field;
select * from field_of_study where name='Database';
```

```sh
export database=scigene_database_field
python scigene_field.py
python extract_citation_timeseries.py
python update_top_author_field_hIndex.py 
```

```sh
create table MACG_papers_tmp select P.* from MACG.papers as P join MACG_papers_tmpid as D on P.paperID = D.paperID
Traceback (most recent call last):
  File "extract_citation_time_series_MAG.py", line 44, in <module>  
  File "extract_citation_time_series_MAG.py", line 40, in execute
    t = time.time()
  File "/home/sy/anaconda3/lib/python3.8/site-packages/pymysql/cursors.py", line 153, in execute
    result = self._query(query)
  File "/home/sy/anaconda3/lib/python3.8/site-packages/pymysql/cursors.py", line 322, in _query
    conn.query(q)
  File "/home/sy/anaconda3/lib/python3.8/site-packages/pymysql/connections.py", line 558, in query
    self._affected_rows = self._read_query_result(unbuffered=unbuffered)
  File "/home/sy/anaconda3/lib/python3.8/site-packages/pymysql/connections.py", line 822, in _read_query_result
    result.read()
  File "/home/sy/anaconda3/lib/python3.8/site-packages/pymysql/connections.py", line 1200, in read
    first_packet = self.connection._read_packet()
  File "/home/sy/anaconda3/lib/python3.8/site-packages/pymysql/connections.py", line 772, in _read_packet
    packet.raise_for_error()
  File "/home/sy/anaconda3/lib/python3.8/site-packages/pymysql/protocol.py", line 221, in raise_for_error
    err.raise_mysql_exception(self._data)
  File "/home/sy/anaconda3/lib/python3.8/site-packages/pymysql/err.py", line 143, in raise_mysql_exception
    raise errorclass(errno, errval)
pymysql.err.OperationalError: (1206, 'The total number of locks exceeds the lock table size')
```

可能是由于 MACG_papers 和 MACG_papers_tmpid 这两个表的连接导致了锁表的数量超过了 MySQL 的限制。这可能是因为 MACG_papers_tmpid 是一个中间结果表，可能包含了大量的数据，而连接操作需要占用一定数量的锁。将sql操作改为pandas.Dataframe操作，不再报错


# 进程与线程

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

