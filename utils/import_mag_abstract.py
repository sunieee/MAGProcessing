import json
import pymysql
import os
import multiprocessing
from tqdm import tqdm


def parse_nt_line(nt_line):
    # 分割N-Triple行，获取主题、谓词和对象
    subject, predicate, object_literal = nt_line.strip().split(' ', 2)
    
    # 清理和提取entity_id
    entity_id = subject.split('entity/')[-1].strip('>')
    
    # 清理和提取abstract，移除引号和数据类型
    abstract = object_literal.rsplit('^^', 1)[0].strip('"')
    
    return entity_id, abstract

# 示例使用
# nt_line1 = '<http://ma-graph.org/entity/2184927035> <http://purl.org/dc/terms/abstract> "The City of Colombo serves both as the national capital and the largest city in modern Sri Lanka. Colombo and its metropolitan area &#x2014; referred to as the Colombo Metropolitan Region (CMR) &#x2014; fall within the Western Province, which is the most densely populated and economically active region within the country (see Table 1). Transportation activity within this region is also the densest in Sri Lanka."^^<http://www.w3.org/2001/XMLSchema#string> .'
# entity_id, abstract = parse_nt_line(nt_line1)
# print("Entity ID:", entity_id)
# print("Abstract:", abstract)


def create_connection():
    conn = pymysql.connect(host='localhost',
                                user='root',
                                password='root',
                                db='MACG',
                                charset='utf8mb4')
    return conn, conn.cursor()

def import_abstract_from_file(filename):
    print(f'* processing {filename}')
    conn, cursor = create_connection()
    with open(filename, 'r', encoding='utf-8') as f:
        content = []
        previous_line = ''
        for line in f:  # 逐行读取以节省内存
            if line.startswith('<http://ma-graph.org/entity/'):
                if previous_line:  # 如果有前一行，则添加到内容中
                    content.append(previous_line)
                previous_line = line  # 设置当前行为前一行以供下次迭代使用
            else:
                previous_line = previous_line.strip() + ' ' + line.strip()  # 合并行

        if previous_line:  # 确保添加最后一行
            content.append(previous_line)
    
    ix = 0
    for line in tqdm(content, total=len(content)):
        try:
            paperID, abstract = parse_nt_line(line)
        except Exception as e:
            print(e, line)
        
        try:
            sql = f"""
            INSERT INTO abstracts_openalex (paperID, abstract) 
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE abstract = %s;
            """
            cursor.execute(sql, (paperID, abstract, abstract))
            ix += 1
            if ix % 1000 == 0:
                conn.commit()
        except Exception as e:
            print(e, paperID, abstract)
        
        

    conn.close()

dirname = '/home/datahouse/'
files = [dirname + x for x in os.listdir(dirname) if x.startswith('input_chunk_')]
print(f'processing {len(files)} files')
print('process:', multiprocessing.cpu_count())

# for filename in files:
#     import_abstract_from_file(filename)
with multiprocessing.Pool(processes=multiprocessing.cpu_count() * 2) as pool:
    pool.map(import_abstract_from_file, files)