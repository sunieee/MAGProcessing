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


# def batch_retrieve(args):
#     offset, batch_size = args
#     conn, cursor = create_connection()
#     print(f'* batch_retrieve: offset={offset}, batch_size={batch_size}')

#     try:
#         sql = "SELECT paperID FROM abstracts_openalex LIMIT %s OFFSET %s;"
#         cursor.execute(sql, (batch_size, offset))
#         result = cursor.fetchall()
#         return [row['paperID'] for row in result]
#     finally:
#         conn.close()

# total_batches = 3000  # 你想要检索的批次总数
# batch_size = 10000  # 每批次检索的行数
# with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
#     results = pool.map(batch_retrieve, [(i * batch_size, batch_size) for i in range(total_batches)])

# # search all paperID in abstracts_openalex
# # cursor.execute("""SELECT paperID FROM abstracts_openalex;""")
# paperIDs = set([item for sublist in results for item in sublist]) 
# print(f'paperIDs length: {len(paperIDs)}')


def import_abstract_from_file(filename):
    print(f'* processing {filename}')
    with open(filename, 'r') as f:
        data = json.load(f)
    conn, cursor = create_connection()

    paperIDs = set()

    for ix, paperID in tqdm(enumerate(data), total=len(data)):
        # insert into abstracts_openalex
        # if paperID in paperIDs:
        #     continue
        abstract = data[paperID]
        if len(abstract) == 0:
            continue
        if abstract[:8] in ['Abstract', 'ABSTRACT', 'abstract']:
            abstract = abstract[8:].strip()
        
        sql = f"""
        INSERT INTO abstracts (paperID, abstract) 
        VALUES (%s, %s)
        """
        # ON DUPLICATE KEY UPDATE paperID = paperID;
        # 
        try:
            cursor.execute(sql, (paperID, abstract))
            paperIDs.add(paperID)
            if ix % 100 == 0:
                conn.commit()
        except Exception as e:
            print(e, paperID, abstract)
    
    filename = filename.replace('out', 'out/paperID')
    with open(f'{filename}.txt', 'w') as f:
        f.write('\n'.join(paperIDs))

    conn.close()

dirname = 'out/'
files = [dirname + x for x in os.listdir(dirname)]
print(f'processing {len(files)} files')
print('process:', multiprocessing.cpu_count())

# for filename in files:
#     import_abstract_from_file(filename)
with multiprocessing.Pool(processes=multiprocessing.cpu_count() * 2) as pool:
    pool.map(import_abstract_from_file, files)