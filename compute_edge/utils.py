import os
import pymysql
import time
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd

fieldName = os.environ.get('fieldName', 'visualization')

# 对于 authorID 的限制
authorID_list = ['2147343253', '2076420186', '2122885999', 
                 '2003408012', '2762167099', '2158935544', '3206897746']
# authorID_list = ['3206897746']
# 3206897746    2147343253
ids_string = ', '.join(map(str, authorID_list))
filterCondition = f"authorID IN ({ids_string})"
print(filterCondition)


citation_link_table = 'paper_reference_field'
citation_timeseries_table = 'papers_field_citation_timeseries'


database = f"scigene_{fieldName}_field"
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
        print('executing: ', _sql)
        t = time.time()
        cursor.execute(_sql)
        conn.commit()
        print('[time cost: ', time.time()-t, ']')

def executeFetch(sql):
    sql = sql.strip()
    t = time.time()
    cursor.execute(sql)
    rows = cursor.fetchall()
    print('[time cost: ', time.time()-t, ']')
    return rows


# extract all edges of selflink in authorID_list
edges = pd.DataFrame(columns=['authorID', 'citingpaperID', 'citedpaperID'])
for authorID in authorID_list:
    selected_paper_author = pd.read_sql_query(f"""select * from paper_author_field where authorID='{authorID}'""", engine)
    selected_paper = selected_paper_author[['paperID']].drop_duplicates()

    selected_edges = pd.read_sql_query(f"""select * from paper_reference_field
        where citingpaperID in {tuple(selected_paper['paperID'])}
        and citedpaperID in {tuple(selected_paper['paperID'])}""", engine) 
    selected_edges['authorID'] = authorID
    selected_edges = selected_edges[['authorID', 'citingpaperID', 'citedpaperID']]
    print(authorID, len(selected_edges))
    edges = pd.concat([edges, selected_edges])

edges_by_citing = edges.set_index('citingpaperID')
edges_by_cited = edges.set_index('citedpaperID')

nodes = pd.concat([edges['citingpaperID'], edges['citedpaperID']])
nodes = tuple(nodes.drop_duplicates().values)

print('nodes:', len(nodes))

