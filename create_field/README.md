
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


# 构建样例

创建scigene_database_field、scigene_physics_field两个数据库，分别对于的fieldID为：77088390、121332964

## scigene_database_field

以database为例：

```sql
create database scigene_database_field;
select * from field_of_study where name='Database';
```

```sh
# select * from papers_field where fieldID = 77088390;
python scigene_field.py 77088390 scigene_database_field
```

> 575106
100%|███████████████████████████████████████████████████████████████████████| 5752/5752 [10:29<00:00,  9.14it/s]
papers(paperID) original (575106, 8)
papers(paperID) drop_duplicates (575106, 8)
papers(paperID) time cost: 630.5266470909119
100%|████████████████████████████████████████████████████████████████████████| 5752/5752 [04:46<00:00, 20.05it/s]
paper_author(paperID) original (1572976, 3)
paper_author(paperID) drop_duplicates (1564429, 3)
paper_author(paperID) time cost: 288.00314688682556
100%|████████████████████████████████████████████████████████████████████████| 5752/5752 [22:28<00:00,  4.27it/s]
paper_reference(citingpaperID) original (5106008, 2)
paper_reference(citingpaperID) drop_duplicates (5106008, 2)
paper_reference(citingpaperID) time cost: 1352.2168371677399
100%|███████████████████████████████████████████████████████████████████████| 5752/5752 [34:29<00:00,  2.78it/s]
paper_reference(citedpaperID) original (6063437, 2)
paper_reference(citedpaperID) drop_duplicates (6063437, 2)
paper_reference(citedpaperID) time cost: 2073.8136801719666
paper_reference original (11169445, 2)
paper_reference drop_duplicates (10245453, 2)
100%|█████████████████████████████████████████████████████████████████████| 11334/11334 [24:15<00:00,  7.79it/s]
authors(authorID) original (1133315, 5)
authors(authorID) drop_duplicates (1133315, 5)
authors(authorID) time cost: 1456.4913499355316

```sh
python extract_citation_timeseries.py 77088390 scigene_database_field
```


> create table MACG_papers_tmp select P.* from MACG.papers as P join MACG_papers_tmpid as D on P.paperID = D.paperID
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


可能是由于 MACG_papers 和 MACG_papers_tmpid 这两个表的连接导致了锁表的数量超过了 MySQL 的限制。这可能是因为 MACG_papers_tmpid 是一个中间结果表，可能包含了大量的数据，而连接操作需要占用一定数量的锁。

将sql操作改为pandas.Dataframe操作，不再报错，输出结果：

```sh
updating papers_field
executing:  update papers_field set year = year(PublicationDate)
time cost:  1.3622636795043945
executing:  alter table papers_field add index(year)
time cost:  6.731650352478027
executing:  ALTER TABLE papers_field ADD citationCountByYear varchar(999)
time cost:  0.18017005920410156
executing:  update papers_field as PA, papers_field_citation_timeseries as PM set PA.citationCountByYear = PM.citationCountByYear where PA.paperID = PM.paperID
time cost:  37.3734986782074
updating authors_field
executing:  create table authors_field_tmp select tmp.*, @curRank := @curRank + 1 AS authorRank from (select authorID, count(*) as PaperCount_field from paper_author_field group by authorID order by PaperCount_field desc) as tmp, (SELECT @curRank := 0) r
time cost:  43.6377899646759
executing:  create index author_index on authors_field_tmp(authorID)
time cost:  15.976773500442505
executing:  ALTER TABLE authors_field ADD PaperCount_field INT
time cost:  0.04614853858947754
executing:  ALTER TABLE authors_field ADD authorRank INT
time cost:  0.049346923828125
executing:  update authors_field, authors_field_tmp set authors_field.PaperCount_field = authors_field_tmp.PaperCount_field, authors_field.authorRank = authors_field_tmp.authorRank where authors_field.authorID = authors_field_tmp.authorID
time cost:  122.98540711402893
executing:  alter table authors_field add index(authorRank)
time cost:  7.5114710330963135
executing:  drop table authors_field_tmp
time cost:  1.9482250213623047
executing:  create table authors_field_tmp select sum(P.citationCount) as CitationCount_field,  authorID from papers_field as P join paper_author_field as PA on P.paperID = PA.paperID and P.CitationCount >=0 group by authorID
time cost:  41.06043577194214
executing:  create index id_index on authors_field_tmp(authorID)
time cost:  30.40918254852295
executing:  ALTER TABLE authors_field ADD CitationCount_field INT
time cost:  0.18607759475708008
executing:  update authors_field, authors_field_tmp set authors_field.CitationCount_field = authors_field_tmp.CitationCount_field where authors_field.authorID = authors_field_tmp.authorID
time cost:  86.77558755874634
executing:  drop table authors_field_tmp
time cost:  2.4010915756225586
executing:  ALTER TABLE authors_field ADD hIndex_field INT
time cost:  0.059279441833496094
executing:  create table paper_reference_field_labeled(
citingpaperID varchar(15),
citedpaperID varchar(15),
extends_prob double
)
time cost:  0.0490565299987793
executing:  ALTER TABLE authors_field ADD FellowType varchar(999)
time cost:  0.05303478240966797
executing:  update authors_field as af, scigene_acl_anthology.fellow as f set af.FellowType='1' where af.name = f.name and af.authorRank<=1000 and f.type=1 and CitationCount_field>=1000
time cost:  5.21049952507019
```

```sh
python update_top_author_field_hIndex.py 77088390
```


## scigene_physics_field

输出结果：
```sh
6:34:05
papers(paperID) original (735937, 8)
papers(paperID) drop_duplicates (729964, 8)
papers(paperID) time cost: 23722.209959745407

7:09:55
paper_author(paperID) original (1901573, 3)
paper_author(paperID) drop_duplicates (1874240, 3)
paper_author(paperID) time cost: 25926.57680273056

4:54:40
paper_reference(citingpaperID) original (3324155, 2)
paper_reference(citingpaperID) drop_duplicates (3296656, 2)
paper_reference(citingpaperID) time cost: 17701.392360925674

4:03:35
paper_reference(citedpaperID) original (2907791,2)              
paper_reference(citedpaperID) drop_duplicates (2884629, 2)       
paper_reference(citedpaperID) time cost: 14619.974665641785      
paper_reference original (6181285, 2)                            
paper_reference drop_duplicates (6132999, 2)  

45:14
authors(authorID) original (1380950, 5)                          
authors(authorID) drop_duplicates (1380950, 5)                   
authors(authorID) time cost: 2716.215271949768 

(1380950, 5)                                                       
executing:  ALTER TABLE papers_field ADD CONSTRAINT papers_field_pk PRIMARY KEY (paperID)                                              
time cost:  21.612918376922607                                     
executing:  alter table papers_field add index(citationCount)                                                                          
time cost:  7.2761664390563965                                     
executing:  alter table paper_author_field add index(paperID)                                                                          
time cost:  11.043795347213745                                     
executing:  alter table paper_author_field add index(authorID)                                                                         
time cost:  11.775491714477539                                     
executing:  alter table paper_author_field add index(authorOrder)                                                                      
time cost:  9.764228582382202                                      
executing:  alter table authors_field add index(authorID)                                                                              
time cost:  9.621068239212036                                      
executing:  alter table authors_field add index(name)                                                                                  
time cost:  10.422689199447632                                     
executing:  alter table paper_reference_field add index(citingpaperID) 
time cost:  28.47778820991516
executing:  alter table paper_reference_field add index(citedpaperID)
time cost:  29.47544527053833
executing:  ALTER TABLE paper_reference_field ADD CONSTRAINT paper_reference_field_pk PRIMARY KEY (citingpaperID,citedpaperID)
time cost:  133.8593955039978
creating papers_field_citation_timeseries_raw_raw
creating MACG_papers_tmpid
creating MACG_papers_tmp
updating year
creating papers_field_citation_timeseries_raw
deleting rows with year <= 0
selecting all raw paper years
processing each citation count
inserting into papers_field_citation_timeseries
100%|████████████████████████████████████████| 31931/31931 [00:08<00:00, 3756.60it/s]
updating papers_field
executing:  update papers_field set year = year(PublicationDate)
time cost:  21.70390558242798
executing:  alter table papers_field add index(year)
time cost:  12.307689428329468
executing:  ALTER TABLE papers_field ADD citationCountByYear varchar(999)
time cost:  0.09853434562683105
executing:  update papers_field as PA, papers_field_citation_timeseries as PM set PA.citationCountByYear = PM.citationCountByYear where PA.paperID = PM.paperID
time cost:  10.841958999633789
updating authors_field
executing:  create table authors_field_tmp select tmp.*, @curRank := @curRank + 1 AS authorRank from (select authorID, count(*) as PaperCount_field from paper_author_field group by authorID order by PaperCount_field desc) as tmp, (SELECT @curRank := 0) r
time cost:  15.048639297485352
executing:  create index author_index on authors_field_tmp(authorID)
time cost:  15.170480966567993
executing:  ALTER TABLE authors_field ADD PaperCount_field INT
time cost:  0.06644439697265625
executing:  ALTER TABLE authors_field ADD authorRank INT
time cost:  0.08142232894897461
executing:  update authors_field, authors_field_tmp set authors_field.PaperCount_field = authors_field_tmp.PaperCount_field, authors_field.authorRank = authors_field_tmp.authorRank where authors_field.authorID = authors_field_tmp.authorID
time cost:  152.96477723121643
executing:  alter table authors_field add index(authorRank)
time cost:  9.41012167930603
executing:  drop table authors_field_tmp
time cost:  0.7245469093322754
executing:  create table authors_field_tmp select sum(P.citationCount) as CitationCount_field,  authorID from papers_field as P join paper_author_field as PA on P.paperID = PA.paperID and P.CitationCount >=0 group by authorID
time cost:  77.19298481941223
executing:  create index id_index on authors_field_tmp(authorID)
time cost:  14.674506902694702
executing:  ALTER TABLE authors_field ADD CitationCount_field INT
time cost:  0.09812712669372559
executing:  update authors_field, authors_field_tmp set authors_field.CitationCount_field = authors_field_tmp.CitationCount_field where authors_field.authorID = authors_field_tmp.authorID
time cost:  119.2219545841217
executing:  drop table authors_field_tmp
time cost:  0.9966509342193604
executing:  ALTER TABLE authors_field ADD hIndex_field INT
time cost:  0.07900786399841309
executing:  create table paper_reference_field_labeled(
citingpaperID varchar(15),
citedpaperID varchar(15),
extends_prob double
)
time cost:  0.05626392364501953
executing:  ALTER TABLE authors_field ADD FellowType varchar(999)
time cost:  0.09914207458496094
executing:  update authors_field as af, scigene_acl_anthology.fellow as f set af.FellowType='1' where af.name = f.name and af.authorRank<=1000 and f.type=1 and CitationCount_field>=1000
time cost:  2.1279916763305664
```
