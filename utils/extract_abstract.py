import pandas as pd
import multiprocessing
import json
import os
from tqdm import tqdm

#################################################################
# 提取OpenAlex中摘要
# https://docs.openalex.org/api-entities/works/work-object


def parse(content, verbose=False):
    content = content.strip('"').replace('""', '"')
    content = json.loads(content)
    if verbose:
        json.dump(content, open('example.json', 'w'), indent=4, sort_keys=True, ensure_ascii=False)

    order2name = {}
    for name, orders in content.items():
        for order in orders:
            order2name[order] = name

    order2name = sorted(order2name.items(), key=lambda x: x[0])
    if verbose:
        print(order2name)

    abstract = ''
    last_order = order2name[0][0]
    for order, name in order2name:
        # if order - last_order > 1:
        #     # abstract += '\n'
        #     abstract += '[MISSING]' + ' '
        abstract += name + ' '
        last_order = order

    return abstract[:-1]


# with open('example.txt', 'r') as f:
#     content = f.read()
# print(parse(content, verbose=True))

os.makedirs('out', exist_ok=True)

# id,doi,title,display_name,publication_year,publication_date,type,cited_by_count,is_retracted,is_paratext,cited_by_api_url,abstract_inverted_index
def process_file(filename):
    print(f'* processing {filename}')
    df = pd.read_csv(filename,  usecols=[0, 4, 11], header=None)
    file = filename.split('/')[-1]
    print(f'[{file}] read complete')
    dic = {}
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        key = row[0]
        try:
            year = int(row[4])
        except:
            year = 0
        if key.startswith('https://openalex.org/W') and year >= 2019:
            key = key.split('/W')[-1]
            abstract = row[11]
            try:
                abstract = parse(abstract)
            except:
                abstract = ''
            # print(key, value)
            dic[key] = abstract

    print(f'dump to out/{file}.json')
    with open(f'out/{file}.json', 'w') as f:
        json.dump(dic, f, indent=4, sort_keys=True, ensure_ascii=False)


# 获取文件的总行数
# filename = '/data/work/input_chunk_aa'
# total_rows = sum(1 for _ in open(filename)) - 1  # 减1是为了去掉标题行

dirname = '/data/work/'
files = [dirname + x for x in os.listdir(dirname) if x.startswith('input_chunk_')]
print('len files:', len(files))
num_processes = multiprocessing.cpu_count() - 1

# 使用多进程读取文件
with multiprocessing.Pool(processes=num_processes) as pool:
    pool.map(process_file, files)

# for file in files:
#     process_file(file)