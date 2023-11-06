
# 提取并导入openalex abstract

拷贝并拆分csv：

```sh
cd /datahouse/openalex/csv-files
cp -i works.csv /data/work

while pgrep -f "cp -i works.csv /data/work" > /dev/null; do
    echo "Waiting for cp to finish at $(date)"
    sleep 10
done

split -l 1000000 works.csv input_chunk_
```

多进程处理csv输出为json: `python extract_abstract.py`

同步到140服务器：`rsync -a out/ 192.168.0.140:/home/sy/arc/utils/out/ --progress`

`watch -n 1 "du -h --max-depth=1"`

创建abstract_openalex表：
```
CREATE TABLE `MACG`.`abstract_openalex` (
  `paperID` VARCHAR(15) NOT NULL,
  `abstract` MEDIUMTEXT NULL,
  PRIMARY KEY (`paperID`));
```

多进程导入abstract_openalex表：`python import_abstract.py > out/import_abstract.log`

# utf-8编码

> `utf8mb3`是MySQL中`utf8`编码的一个别名，它是一个最多能够支持3个字节的UTF-8编码。这意味着它可以支持Unicode中的大多数字符，包括多种语言的字符、标点符号、数学符号等。然而，`utf8mb3`不支持包括一些表情符号、古文字、一些未常用的语言字符和符号在内的“四字节字符”。
>
> 从MySQL 5.5.3版本开始，引入了`utf8mb4`编码，它支持“四字节字符”。`utf8mb4`是真正的UTF-8编码，可以支持所有Unicode字符，包括那些需要4个字节存储的特殊字符。
>
> 如果你需要存储表情符号或其他一些特殊的Unicode字符，你应该使用`utf8mb4`而不是`utf8mb3`。例如，现代的应用程序，如社交媒体平台，通常需要存储用户输入的表情符号，这时就必须使用`utf8mb4`编码。
> 
> 在你的情况下，如果你的数据包含四字节的UTF-8字符（如某些表情符号），你应该确保数据库、表和连接都使用`utf8mb4`编码。如果你尝试在`utf8mb3`编码的列中插入四字节字符，你会遇到错误，因为这些字符无法在`utf8mb3`中正确存储。

由于abstract数据表是`utf8mb3`编码，不支持新的特殊字符，因此，按 PaperAbstracts.csv，有80%以上的论文（191M/237M）都有abstract，但如果按mysql的paper_Abstracts表，只有57%论文（138M/237M）有abstract。需要重新导入！

1. 确认数据表abstract字段的字符编码是utf8mb4
```sql
ALTER DATABASE MACG CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
ALTER TABLE abstracts CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

# docker cp my.cnf mysql:/etc/mysql/conf.d/

# mysql -u username -p --default-character-set=utf8mb4
LOAD DATA INFILE 'path/to/PaperAbstracts.csv' INTO TABLE abstracts
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
```

2. 导入过程中，需要确保数据库连接也使用的是utf8mb4字符集。在Python中，如果你使用的是mysql-connector-python，可以在连接时指定字符集：charset='utf8mb4'

3. 客户端和服务器的字符集设置：
确保MySQL服务器的字符集设置也是utf8mb4。你可以通过以下命令检查和设置：

```sql
SET NAMES 'utf8mb4';
SET CHARACTER SET utf8mb4;
```

4. Python字符串的处理：
在将字符串传递给MySQL之前，确保它们在Python中是以正确的形式处理的。如果你从CSV文件读取数据，确保读取时使用了正确的编码：

```python
with open('yourfile.csv', encoding='utf-8') as f:
    # Your reading logic
```





多进程导入abstract_openalex表：`python import_mag_abstract.py > out/import_mag_abstract.log`


多进程整理abstract, 导入abstract_new表：`python clean_abstract_table.py > out/clean_abstract_table.log`