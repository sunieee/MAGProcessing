import pandas as pd
import pymysql
from datetime import datetime
import os


def create_connection(database='MACG'):
    conn = pymysql.connect(host='localhost',
                            user='root',
                            password='root',
                            db=database,
                            charset='utf8')
    return conn, conn.cursor()

field = os.environ.get('field')
database = os.environ.get('database', 'MACG')

conn, cursor = create_connection()
df_authors = pd.read_csv(f'out/{field}/authors.csv')
df_authors['authorID'] = df_authors['authorID'].astype(str)

authorID_list = set(df_authors['authorID'].tolist())
authorIDs = authorID_list
authorID_str = ','.join([f'\'{x}\'' for x in authorID_list])

print('loading data from database', datetime.now().strftime("%H:%M:%S"))
path = f'out/{field}/csv'
if not os.path.exists(path):
    os.makedirs(path)
    if database == 'MACG':
        df_paper_author = pd.read_sql_query(f"select * from paper_author where authorID in ({authorID_str})", conn)
        df_paper_author.to_csv(f"{path}/paper_author.csv", index=False)
        paperID_list = set(df_paper_author['paperID'].tolist())
        paperID_str = ','.join([f'\'{x}\'' for x in paperID_list])
        #
        df_papers = pd.read_sql_query(f"select * from papers where paperID in ({paperID_str})", conn)
        df_papers.to_csv(f"{path}/papers.csv", index=False)
        #
        df_paper_reference = pd.concat([
            pd.read_sql_query(f"select * from paper_reference where citingpaperID in ({paperID_str})", conn),
            pd.read_sql_query(f"select * from paper_reference where citedpaperID in ({paperID_str})", conn)
        ])
        df_paper_reference.to_csv(f"{path}/paper_reference.csv", index=False)
        df_paper_reference = None
    else:
        df_paper_author = pd.read_csv(f"out/{database}/csv/paper_author.csv")
        df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
        df_paper_author = df_paper_author[df_paper_author['authorID'].isin(authorID_list)]
        df_paper_author.to_csv(f"{path}/paper_author.csv", index=False)
        paperIDs = set(df_paper_author['paperID'].tolist())

        df_papers = pd.read_csv(f"out/{database}/csv/papers.csv")
        df_papers = df_papers[df_papers['paperID'].isin(paperIDs)]
        df_papers.to_csv(f"{path}/papers.csv", index=False)

        df_paper_reference = pd.read_csv(f"out/{database}/csv/paper_reference.csv")
        df_paper_reference = df_paper_reference[df_paper_reference['citingpaperID'].isin(paperIDs) | df_paper_reference['citedpaperID'].isin(paperIDs)]
        df_paper_reference.to_csv(f"{path}/paper_reference.csv", index=False)
        df_paper_reference = None
else:
    df_paper_author = pd.read_csv(f"{path}/paper_author.csv")
    df_papers = pd.read_csv(f"{path}/papers.csv")
    
df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
df_paper_author['paperID'] = df_paper_author['paperID'].astype(str)
df_papers['paperID'] = df_papers['paperID'].astype(str)

if 'PaperCount_field' not in df_authors.columns:
    paper_count = df_paper_author.groupby('authorID')['paperID'].count().reset_index(name='PaperCount_field')
    df_authors = df_authors.merge(paper_count, on='authorID', how='left')
    df_authors['PaperCount_field'] = df_authors['PaperCount_field'].fillna(0)
    df_authors.to_csv(f'out/{field}/authors.csv',index=False)

df_papers['PublicationDate'] = pd.to_datetime(df_papers['PublicationDate'])
df_papers['year'] = df_papers['PublicationDate'].dt.year

