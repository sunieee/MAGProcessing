import os
import pymysql
import time
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
from tqdm import tqdm

database = os.environ.get('database', 'scigene_visualization_field')

filterCondition = f"PaperCount_field > 10"

# 对于 authorID 的限制
# authorID_list = ['2147343253', '2076420186', '2122885999', 
#                  '2003408012', '2762167099', '2158935544', '3206897746']
# authorID_list = ['3206897746']
# 3206897746    2147343253
# ids_string = ', '.join(map(str, authorID_list))
# filterCondition = f"authorID IN ({ids_string})"
# print(filterCondition)

citation_link_table = 'paper_reference_field'
citation_timeseries_table = 'papers_field_citation_timeseries'

engine = create_engine(f"mysql+pymysql://root:root@localhost:3306/{database}?charset=utf8")
conn = pymysql.connect(host='localhost',
                            port=3306,
                            user='root',
                            password='root',
                            db=database,
                            charset='utf8')
cursor = conn.cursor()
def execute(sql):
    for _sql in sql.split(';'):
        _sql = _sql.strip()
        if _sql == '':
            continue
        print('* execute', _sql)
        t = time.time()
        cursor.execute(_sql)
        conn.commit()
        print('[time cost: ', time.time()-t, ']')


def executeFetch(sql):
    sql = sql.strip()
    print('* executeFetch', sql)
    t = time.time()
    cursor.execute(sql)
    rows = cursor.fetchall()
    print('[time cost: ', time.time()-t, ']')
    return rows


authors_df = pd.read_sql_query(f"""select * from authors_field where {filterCondition}""", engine)
authorID_list = tuple(authors_df['authorID'].values)

# 直接从paper_reference_field表中筛选出自引的记录
if os.path.exists(f'out/{database}/edges.csv'):
    edges = pd.read_csv(f'out/{database}/edges.csv')
else:
    print('edges.csv not found, creating self-reference graph...')
    t = time.time()
    edges = pd.read_sql_query(f"""
        select prf.*, paf1.authorID
        from paper_reference_field prf
        join paper_author_field paf1 on prf.citingpaperID = paf1.paperID
        join paper_author_field paf2 on prf.citedpaperID = paf2.paperID
        where paf1.authorID = paf2.authorID
        and paf1.authorID in {authorID_list}
        and paf2.authorID in {authorID_list}
    """, engine)
    print(f'edges created, time cost:', time.time()-t)

    edges = edges[['authorID', 'citingpaperID', 'citedpaperID']]
    edges.to_csv(f'out/{database}/edges.csv', index=False)

edges['authorID'] = edges['authorID'].astype(str)
edges['citingpaperID'] = edges['citingpaperID'].astype(str)
edges['citedpaperID'] = edges['citedpaperID'].astype(str)
edges_by_citing = edges.set_index('citingpaperID')
edges_by_cited = edges.set_index('citedpaperID')

nodes = pd.concat([edges['citingpaperID'], edges['citedpaperID']])
nodes = tuple(nodes.drop_duplicates().values)

print('nodes:', len(nodes), 'edges:', len(edges))

