import json
import pymysql
import os
import multiprocessing
from tqdm import tqdm


def create_connection():
    conn = pymysql.connect(host='localhost',
                                user='root',
                                password='root',
                                db='MACG',
                                charset='utf8mb4')
    return conn, conn.cursor()

# 连接到数据库并获取所有不同的paperID
def get_paperIDs():
    conn, cursor = create_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT paperID FROM abstracts;")
            return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()

# 处理单个paperID
def process_paperID(paperIDs):
    conn, cursor = create_connection()
    count = 0
    for paperID in tqdm(paperIDs):
        try:
            # 读取所有与此paperID相关的abstracts
            cursor.execute("SELECT abstract FROM abstracts WHERE paperID = %s;", (paperID,))
            abstracts = cursor.fetchall()
            
            # 合并abstracts
            unique_abstracts = set(abs[0] for abs in abstracts)  # 去除重复的abstract
            merged_abstract = '\n'.join(unique_abstracts)
            
            # 插入到新表中
            cursor.execute("""
                INSERT INTO MACG_new.abstracts_new (paperID, abstract)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE abstract=VALUES(abstract);
            """, (paperID, merged_abstract))
            
            count += 1
            if count % 100 ==0:
                conn.commit()
        except Exception as e:
            print(e, paperID)
    
    conn.close()


conn, cursor = create_connection()
cursor.execute('''CREATE TABLE IF NOT EXISTS abstracts_new (
paperID VARCHAR(15) NOT NULL,
abstract MEDIUMTEXT,
PRIMARY KEY (paperID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')
conn.close()

if os.path.exists('paperID.txt'):
    with open('out/paperID.txt', 'r') as f:
        paperIDs = f.read().split('\n')
else:
    paperIDs = get_paperIDs()
    with open('out/paperID.txt', 'w') as f:
        f.write('\n'.join(paperIDs))


count = 100000
with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
    pool.map(process_paperID, [paperIDs[i:i+count] for i in range(0, len(paperIDs), count)])

# sudo docker update --memory "128g" --memory-swap "256g" 1ee3d3a9e719
# sudo docker update --restart=always 1ee3d3a9e719
